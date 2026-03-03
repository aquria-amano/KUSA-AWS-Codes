import pkg from "@aws-sdk/client-sns";
const { SNSClient } = pkg;

const snsClient = new SNSClient({ region: "ap-northeast-1" });

export const handler = async (event) => {
    console.log("Received full event:", JSON.stringify(event, null, 2));

    const ARN_PROD = "arn:aws:sns:ap-northeast-1:518045402321:app/APNS/AppleArcadeKUSA-Production";
    const ARN_SANDBOX = "arn:aws:sns:ap-northeast-1:518045402321:app/APNS_SANDBOX/AppleArcadeKUSA";
    const TOPIC_ALL = "arn:aws:sns:ap-northeast-1:518045402321:kusa-AllUserTopic";
    const TOPIC_LANG_PREFIX = "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-";

    try {
        const body = JSON.parse(event.body || "{}");
        const deviceToken = body.token;
        const userId = body.userId || "unknown_user";
        const language = body.language || "en-US";
        const isProduction = body.isProduction === true;
        const oldSubscriptionArn = body.oldSubscriptionArn; // Unityから送られてくる前回ID
        const targetPlatformArn = isProduction ? ARN_PROD : ARN_SANDBOX;

        if (!deviceToken) {
            return { statusCode: 400, body: JSON.stringify({ error: "Device token is required" }) };
        }

        let endpointArn;
        
        // 1. エンドポイントの作成または更新
        try {
            const res = await snsClient.send(new pkg.CreatePlatformEndpointCommand({
                PlatformApplicationArn: targetPlatformArn,
                Token: deviceToken,
                CustomUserData: userId
            }));
            endpointArn = res.EndpointArn;
        } catch (innerErr) {
            if (innerErr.name === 'InvalidParameterException' && innerErr.message.includes('already exists')) {
                const match = innerErr.message.match(/Endpoint (arn:aws:sns[^ ]+) already exists/);
                endpointArn = match ? match[1] : null;
                if (endpointArn) {
                    await snsClient.send(new pkg.SetEndpointAttributesCommand({
                        EndpointArn: endpointArn,
                        Attributes: { CustomUserData: userId }
                    }));
                }
            } else { throw innerErr; }
        }

        // 2. 共通トピックへの登録
        await snsClient.send(new pkg.SubscribeCommand({
            TopicArn: TOPIC_ALL,
            Protocol: 'application',
            Endpoint: endpointArn
        }));

        // 3. 新しい言語別トピックへの登録
        const targetLangTopicArn = `${TOPIC_LANG_PREFIX}${language}`;
        const subRes = await snsClient.send(new pkg.SubscribeCommand({
            TopicArn: targetLangTopicArn,
            Protocol: 'application',
            Endpoint: endpointArn
        }));
        
        const currentSubscriptionArn = subRes.SubscriptionArn; // 今回の登録ID

        // --- 4. 古い言語トピックの購読解除（ピンポイント削除 + リスト検索） ---
        
        // A. Unityから送られてきた古いIDをピンポイントで削除
        if (oldSubscriptionArn && oldSubscriptionArn.startsWith("arn:aws:sns")) {
            // 今回の登録IDと違う場合のみ削除実行
            if (oldSubscriptionArn !== currentSubscriptionArn) {
                try {
                    console.log(`Pinpoint Unsubscribe: ${oldSubscriptionArn}`);
                    await snsClient.send(new pkg.UnsubscribeCommand({
                        SubscriptionArn: oldSubscriptionArn
                    }));
                } catch (e) {
                    console.log("Pinpoint Unsubscribe skipped (already gone or invalid):", e.message);
                }
            }
        }

        // B. 保険としてリストからもクリーンアップ（既存ロジック維持）
        try {
            const subsResponse = await snsClient.send(new pkg.ListSubscriptionsCommand({}));
            if (subsResponse.Subscriptions) {
                const mySubscriptions = subsResponse.Subscriptions.filter(s => s.Endpoint === endpointArn);
                for (const sub of mySubscriptions) {
                    const topicArn = sub.TopicArn;
                    const subArn = sub.SubscriptionArn;

                    // 言語トピックであり、かつ「今回のID」でも「共通トピック」でもないものを解除
                    if (topicArn.startsWith(TOPIC_LANG_PREFIX) && subArn !== currentSubscriptionArn) {
                        if (subArn.startsWith("arn:aws:sns")) { 
                            console.log(`Cleanup Unsubscribe: ${topicArn}`);
                            await snsClient.send(new pkg.UnsubscribeCommand({
                                SubscriptionArn: subArn
                            }));
                        }
                    }
                }
            }
        } catch (unsubErr) {
            console.error("List Cleanup failed:", unsubErr.message);
        }
        
        return {
            statusCode: 200,
            headers: { "Content-Type": "application/json" },
            // subscriptionArnを返却してUnity側に保存させる
            body: JSON.stringify({ 
                result: "success", 
                endpointArn, 
                language, 
                subscriptionArn: currentSubscriptionArn 
            }),
        };
    } catch (error) {
        console.error("Critical Error:", error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};
