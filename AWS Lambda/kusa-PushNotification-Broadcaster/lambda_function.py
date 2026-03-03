import boto3
import json

sns = boto3.client('sns')

def lambda_handler(event, context):
    # --- APIキーによる簡易認証 (API Gateway経由の場合のみ) ---
    SECRET_API_KEY = "tFiMsbMSWuJ6YrfH"
    headers = event.get('headers', {})
    request_api_key = headers.get('x-api-key')
    
    if 'headers' in event:
        if request_api_key != SECRET_API_KEY:
            print("Invalid API Key attempt")
            return {'statusCode': 403, 'body': "Forbidden"}
    
    print(f"Received event: {json.dumps(event)}")
    
    # --- データの取り出し ---
    if 'body' in event:
        try:
            input_data = json.loads(event['body'])
        except Exception as e:
            return {'statusCode': 400, 'body': f"Invalid JSON: {str(e)}"}
    else:
        input_data = event

    results = []
    
    # --- A. 即時通知（単一トピック指定）の場合 ---
    if 'targetTopic' in input_data:
        topic_name = input_data['targetTopic']
        title = input_data.get('title', 'お知らせ')
        body = input_data.get('body', '')
        res = publish_to_topic(topic_name, title, body)
        return {'statusCode': 200, 'body': json.dumps([res])}

    # --- B. 予約通知（地域別一括送信）の場合 ---
    timezone_label = input_data.get('timezoneLabel')
    notifications = input_data.get('notifications', [])

    if not timezone_label or not notifications:
        return {'statusCode': 400, 'body': "Missing timezoneLabel or notifications"}

    for item in notifications:
        lang = item.get('lang')
        title = item.get('title', 'お知らせ')
        body = item.get('body')
        
        # トピック名を組み立て (例: kusa-AsiaEast-ja)
        topic_name = f"kusa-{timezone_label}-{lang}"
        
        res = publish_to_topic(topic_name, title, body)
        results.append(res)
            
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

def publish_to_topic(topic_name, title, body):
    """
    トピック名からARNを取得（作成）し、メッセージを送信する補助関数
    """
    try:
        # トピックを作成または取得（既存ならARNを返すだけ）
        create_res = sns.create_topic(Name=topic_name)
        topic_arn = create_res['TopicArn']
        
        # プッシュ通知用ペイロード
        payload = {
            "default": body,
            "APNS": json.dumps({
                "aps": {
                    "alert": {"title": title, "body": body},
                    "sound": "default",
                    "badge": 1
                }
            }),
            "APNS_SANDBOX": json.dumps({
                "aps": {
                    "alert": {"title": title, "body": body},
                    "sound": "default",
                    "badge": 1
                }
            })
        }
        
        sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            MessageStructure='json'
        )
        return f"{topic_name}: Success"
    except Exception as e:
        print(f"Error publishing to {topic_name}: {str(e)}")
        return f"{topic_name}: Error ({str(e)})"