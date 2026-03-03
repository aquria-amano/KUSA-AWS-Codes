import boto3
import json

sns = boto3.client('sns')

# 16言語分のトピックARN
TOPIC_MAP = {
    "ar": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-ar",
    "de": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-de",
    "en-GB": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-en-GB",
    "en-US": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-en-US",
    "es": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-es",
    "fr-CA": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-fr-CA",
    "fr-FR": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-fr-FR",
    "it": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-it",
    "ja": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-ja",
    "ko": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-ko",
    "nl": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-nl",
    "pt-BR": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-pt-BR",
    "ru": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-ru",
    "tr": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-tr",
    "zh-Hans": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-zh-Hans",
    "zh-Hant": "arn:aws:sns:ap-northeast-1:518045402321:kusa-User-Language-zh-Hant",
}

def lambda_handler(event, context):

    # --- APIキーによる簡易認証 ---
    SECRET_API_KEY = "tFiMsbMSWuJ6YrfH"  # 好きな英数字に書き換えてください
    
    # API Gatewayから届くヘッダーを確認
# --- 修正後の認証ロジック ---
    headers = event.get('headers', {})
    request_api_key = headers.get('x-api-key')
    
    # eventの中に 'headers' がある ＝ API Gateway（外部）からの呼び出し
    # この場合のみ、APIキーの照合を行う
    if 'headers' in event:
        if request_api_key != SECRET_API_KEY:
            print("Invalid API Key attempt") # 今出ているログ
            return {'statusCode': 403, 'body': "Forbidden"}
    
    # 'headers' がない場合（EventBridgeなど）は、内部呼び出しとして信頼して進む
    # ------------------------------

    print(f"Received event: {json.dumps(event)}") # デバッグ用に受信データを出力
    
    # --- 修正ポイント：データの取り出し ---
    # API Gateway経由の場合、データは 'body' というキーの中に文字列で入っています
    if 'body' in event:
        try:
            input_data = json.loads(event['body'])
        except Exception as e:
            return {'statusCode': 400, 'body': f"Invalid JSON in body: {str(e)}"}
    else:
        # テスト実行などの直接呼び出しの場合
        input_data = event
    
    notifications = input_data.get('notifications', [])
    # ----------------------------------

    results = []
    
    for item in notifications:
        lang = item.get('lang')
        title = item.get('title', 'お知らせ')
        body = item.get('body')
        
        if lang in TOPIC_MAP:
            topic_arn = TOPIC_MAP[lang]
            payload = {
                "default": body,
                "APNS": json.dumps({
                    "aps": {
                        "alert": {"title": title, "body": body},
                        "sound": "default",
                        "badge": 1
                    }
                })
            }
            
            try:
                sns.publish(
                    TopicArn=topic_arn,
                    Message=json.dumps(payload),
                    MessageStructure='json'
                )
                results.append(f"{lang}: Success")
            except Exception as e:
                results.append(f"{lang}: Error ({str(e)})")
        else:
            results.append(f"{lang}: Topic not found")
            
    return {
        'statusCode': 200,
        'body': json.dumps(results) # BodyもJSON形式にして返すのがAPI Gatewayの作法です
    }
    