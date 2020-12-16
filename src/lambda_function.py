import requests
import os
import json
import datetime
import boto3
import logging
import datetime
import urllib.request

now = datetime.datetime.now()
now = '-'.join([str((now.hour+9)%24),str(now.minute),str(now.second)])

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # eventが発生したらDBに接続
    
    logging.info(json.dumps(event))
    
    dynamoDB = boto3.resource("dynamodb")
    table_name = 'Asakatsu_Slack_DB'
    table = dynamoDB.Table(table_name)
    
    if 'event_name' in event: # トリガーがCloudWatchのとき
        if event["event_name"] == "send":
            day_check_all_to_false(table)
            post_message_to_channel('朝だよ！僕に返信してね!！！')
            return return_200_message('aggregate start')
        elif event["event_name"] == "aggregate":
            # week_checkを+1したのちday_checkを全員falseにする
            day_result_post(table)
            week_check_plus_1(table)
            day_check_all_to_false(table)
            return return_200_message('day aggregate finish')
        elif event["event_name"] == "weekly_check":
            week_result_post(table)
            week_check_all_to_0(table)
            return return_200_message('week aggregate finish')
    
    elif 'body' in event.keys():
        body = json.loads(event['body'])
    else:
        return_200_message('invalid event')
    if 'authorizations' in body: # トリガーが返信のとき
        
        ## 検証スタート##
        
        #tokenの確認
        if body['token'] != os.environ['SLACK_TOKEN']:
            return return_200_message('invalid token')
        
        # botなら終了
        if 'user' not in body['event'].keys():
            return return_200_message('not user')

        # 投稿者のidを取得
        user_id = body['event']['user']
        
        # 投稿者が登録されてなければ終了
        user_name = os.environ.get(user_id)

        if not user_name:
            return return_200_message('not correct user')
        
        ## 検証終わり##
        
        # 投稿時に何か返信
        # post_message_to_channel('something')
        
        response = table.scan()
        items = response['Items']
        name_list_in_db = [item['name'] for item in items]
        if user_name not in name_list_in_db: # 返信者がもしDBにいなかったら追加
            table.put_item(
                Item = {
                    "day_check":True,
                    "name":user_name,
                    "week_check":int(0),
                    "sum_check":int(0)
                    }
                )
        else: # もしDBにいたらday_checkをTrueに
            primary_key = {
                "name":user_name
            }
            item = table.get_item(
                Key=primary_key
            )
            # もしday_checkがすでにTrueだったら終了
            if item["Item"]['day_check'] == True:
                return return_200_message('already day_check true')
            table.update_item(
                Key = primary_key,
                UpdateExpression="set day_check = :day_check",
                ExpressionAttributeValues = {
                    ":day_check":True
                })
        
        return return_200_message(f'{user_name}'+'オッケー')
    else:
        return return_200_message('not OK')

def post_message_to_channel(message):
    url = os.environ['SLACK_INCOMING_WEBHOOK']
    msg = json.dumps({
        "text": message,
    })
    requests.post(url, data=msg)

def return_200_message(message):
    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }

def day_check_all_to_false(table):
    ### 全員のday_checkをfalseにリセットする
    response = table.scan()
    items = response['Items']
    for item in items:
        primary_key = {
            "name":item['name']
        }
        table.update_item(
            Key = primary_key,
            UpdateExpression="set day_check = :day_check",
            ExpressionAttributeValues = {
                ":day_check":False
            })

def week_check_all_to_0(table):
    ### 全員のweek_checkを0にリセットする
    response = table.scan()
    items = response['Items']
    for item in items:
        primary_key = {
            "name":item['name']
        }
        table.update_item(
            Key = primary_key,
            UpdateExpression="set week_check = :week_check",
            ExpressionAttributeValues = {
                ":week_check":int(0)
            })

def week_check_plus_1(table):
    response = table.scan()
    items = response['Items']
    for item in items:
        if not item['day_check']:continue
        week_check_update = item['week_check'] + 1
        sum_check_update = item['sum_check'] + 1
        primary_key = {
            "name":item['name']
        }
        table.update_item(
            Key = primary_key,
            UpdateExpression="set week_check = :week_check, sum_check = :sum_check",
            ExpressionAttributeValues = {
                ":week_check":week_check_update,
                ":sum_check":sum_check_update
            })
        
def day_result_post(table):
    response = table.scan()
    items = response['Items']
    results = ''
    for item in items:
        name = item['name']
        if item['day_check']:
            results += name + '\n'
    message = \
    '-----------------------------' + '\n' + \
    '⇓ 今日起きれた人 ⇓(test)\n' + \
    results + \
    '-----------------------------'+'\n'
    post_message_to_channel(message)

def week_result_post(table):
    response = table.scan()
    items = response['Items']
    results = ''
    for item in items:
        name = item['name']
        week_check = item['week_check']
        results += name + ' => ' + str(week_check) + '\n'
    message = \
    '-----------------------------' + '\n' + \
    '⇓ 今週の集計 ⇓(test)\n' + \
    results + \
    '-----------------------------'+'\n'
    post_message_to_channel(message)
