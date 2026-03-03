import pkg from "@aws-sdk/client-sns";
const { SNSClient } = pkg;

const snsClient = new SNSClient({ region: "ap-northeast-1" });

// --- タイムゾーン区分マッピングテーブル ---
const TIMEZONE_MAPPING = [
    { label: "AsiaEast",      min: 8.5,  max: 10.0 }, // 日本(+9)
    { label: "AsiaChina",     min: 7.5,  max: 8.4 },  // 中国(+8)
    { label: "AsiaSoutheast", min: 6.5,  max: 7.4 },  // タイ(+7)
    { label: "EuropeCentral", min: 0.5,  max: 2.0 },  // 欧州(+1)
    { label: "GMT",           min: -0.5, max: 0.4 },  // 英国(+0)
    { label: "USEast",        min: -6.0, max: -4.0 }, // NY(-5)
    { label: "USWest",        min: -9.0, max: -7.0 }, // LA(-8)
];

export const handler = async (event) => {
    console.log("Received full event:", JSON.stringify(event, null, 2));

    const ARN_PROD = "arn:aws:sns:ap-northeast-1:518045402321:app/APNS/AppleArcadeKUSA-Production";
    const ARN_SANDBOX = "arn:aws:sns:ap-northeast-1:518045402321:app/APNS_SANDBOX/AppleArcadeKUSA";
    const TOPIC_ALL = "arn:aws:sns:ap-northeast-1:518045402321:kusa-AllUserTopic";
    // 言語トピックのプレフィックスを共通化するために変更
    const TOPIC_PREFIX = "kusa-"; 

    try {
        const body = JSON.parse(event.body || "{}");
        const deviceToken = body.token;
        const userId = body.userId || "unknown_user";
        const language = body.language || "en";
        const timezoneOffset = body.timezoneOffset ?? 0; // Unityから届く数値
        const isProduction = body.isProduction === true;
        const oldSubscriptionArn = body.oldSubscriptionArn;
        const targetPlatformArn = isProduction ? ARN_PROD : ARN_SANDBOX;

        if (!deviceToken) {
            return { statusCode: 400, body: JSON.stringify({ error: "Device token is required" }) };
        }

        // --- A. タイムゾーンラベルの判定 ---
        let selectedLabel = "UTC";
        for (const range of TIMEZONE_MAPPING) {
            if (timezoneOffset >= range.min && timezoneOffset <= range.max) {
                selectedLabel = range.label;
                break;
            }
        }

        // --- B. トピック名の組み立て (kusa-AsiaEast-ja 等) ---
        const targetTopicName = `${TOPIC_PREFIX}${selectedLabel}-${language}`;
        console.log(`Targeting Topic: ${targetTopicName}`);

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

        // 2. 共通トピックへの登録 (既存維持)
        await snsClient.send(new pkg.SubscribeCommand({
            TopicArn: TOPIC_ALL,
            Protocol: 'application',
            Endpoint: endpointArn
        }));

        // 3. 地域別・言語別トピックへの登録
        // まずトピックを作成（または取得）
        const createTopicRes = await snsClient.send(new pkg.CreateTopicCommand({ Name: targetTopicName }));
        const targetTopicArn = createTopicRes.TopicArn;

        const subRes = await snsClient.send(new pkg.SubscribeCommand({
            TopicArn: targetTopicArn,
            Protocol: 'application',
            Endpoint: endpointArn
        }));
        
        const currentSubscriptionArn = subRes.SubscriptionArn;

        // --- 4. 古い購読の解除 ---
        
        // A. ピンポイント削除
        if (oldSubscriptionArn && oldSubscriptionArn.startsWith("arn:aws:sns")) {
            if (oldSubscriptionArn !== currentSubscriptionArn) {
                try {
                    await snsClient.send(new pkg.UnsubscribeCommand({ SubscriptionArn: oldSubscriptionArn }));
                    console.log(`Pinpoint Unsubscribe: ${oldSubscriptionArn}`);
                } catch (e) {
                    console.log("Pinpoint Unsubscribe skipped:", e.message);
                }
            }
        }

        // B. 保険クリーンアップ (kusa- で始まるトピックが対象)
        try {
            const subsResponse = await snsClient.send(new pkg.ListSubscriptionsCommand({}));
            if (subsResponse.Subscriptions) {
                const mySubscriptions = subsResponse.Subscriptions.filter(s => s.Endpoint === endpointArn);
                for (const sub of mySubscriptions) {
                    const tArn = sub.TopicArn;
                    const sArn = sub.SubscriptionArn;

                    // 今回のトピックARNでもなく、ALLトピックでもない「kusa-」系トピックを掃除
                    if (tArn.includes(TOPIC_PREFIX) && tArn !== targetTopicArn && tArn !== TOPIC_ALL && sArn !== currentSubscriptionArn) {
                        if (sArn.startsWith("arn:aws:sns")) { 
                            console.log(`Cleanup Unsubscribe: ${tArn}`);
                            await snsClient.send(new pkg.UnsubscribeCommand({ SubscriptionArn: sArn }));
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
            body: JSON.stringify({ 
                result: "success", 
                endpointArn, 
                appliedTopic: targetTopicName,
                subscriptionArn: currentSubscriptionArn 
            }),
        };
    } catch (error) {
        console.error("Critical Error:", error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};