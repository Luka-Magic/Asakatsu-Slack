import requests
import os
import json
import datetime
import boto3
import logging
import datetime
import urllib.request
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    logging.info(json.dumps(event))

    ## eventが発生したらDBに接続
    dynamoDB = boto3.resource("dynamodb")
    table = dynamoDB.Table(os.environ['DB'])

    ## トリガーがCloudWatchのとき
    if 'event_name' in event:
        # メッセージの受付を開始する
        if event["event_name"] == "accept":
            reset_day_checks(table)
            return ok_message('started accepting message')
        # 朝の訪れを伝える
        elif event["event_name"] == "good_morning":
            post_message_to_channel('朝だよ！僕に返信してね！！')
            return ok_message('sent morning message')
        # メッセージの受付を終了する
        elif event["event_name"] == "aggregate":
            day_results_post(table)
            week_checks_plus1(table)
            reset_day_checks(table)
            return ok_message('stopped accepting message')
        # 一週間の集計結果を投稿する
        elif event["event_name"] == "weekly_result":
            week_results_post(table)
            reset_week_checks(table)
            return ok_message('post week result')
    else:
        try:
            body = json.loads(event['body'])
        except:
            return ok_message('invalid event')
    
    ## トリガーが投稿のとき
    if 'authorizations' in body:
        
        ## 検証スタート##
        
        #tokenの確認
        if body['token'] != os.environ['SLACK_TOKEN']:
            return ok_message('invalid token')
        
        # botなら終了
        if 'user' not in body['event'].keys():
            return ok_message('not user')

        # 投稿者のidを取得
        user_id = body['event']['user']
        
        
        user_name = os.environ.get(user_id)
        
        # 投稿者が登録されてなければ終了
        if not user_name:
            return ok_message('not exist user')
        
        ## 検証終わり##
        
        # 「途中経過」と投稿すると途中経過が見れる
        if "途中経過" == body['event']['text']:
            interim_results_post(table)
            return ok_message('interim results')
        
        response = table.scan()
        items = response['Items']
        name_list_in_db = [item['name'] for item in items]
        
        # 投稿者がもしDBにいなかったらday_checkをTrueにしてDBに追加
        if user_name not in name_list_in_db:
            table.put_item(
                Item = {
                    "day_check":True,
                    "name":user_name,
                    "week_check":int(0),
                    "sum_check":int(0)
                    }
                )
        # すでにDBにいたらday_checkをTrueに
        else:
            primary_key = {
                "name":user_name
            }
            item = table.get_item(
                Key=primary_key
            )
            # すでにday_checkがTrueだったら終了
            if item["Item"]['day_check'] == True:
                return ok_message('day_check already true')
            table.update_item(
                Key = primary_key,
                UpdateExpression="set day_check = :day_check",
                ExpressionAttributeValues = {
                    ":day_check":True
                })
        
        return ok_message(f'{user_name}'+' ok!')
    else:
        return ok_message('not ok')

def post_message_to_channel(message):
    ## メッセージをslackに投稿
    url = os.environ['SLACK_INCOMING_WEBHOOK']
    msg = json.dumps({
        "text": message,
    })
    requests.post(url, data=msg)

def ok_message(message):
    return_message =  {
        'statusCode': 200,
        'body': json.dumps(message)
    }
    logging.info(json.dumps(return_message))
    return return_message

def reset_day_checks(table):
    ### 全員のday_checkをfalseにする
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

def reset_week_checks(table):
    ## 全員のweek_checkを0にする
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

def week_checks_plus1(table):
    ## day_checkがTrueであるメンバーのweek_checkに1を加える
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
        
def day_results_post(table):
    ## 起きれた人を投稿
    response = table.scan()
    items = response['Items']
    results = ''
    for item in items:
        name = item['name']
        if item['day_check']:
            results += name + '\n'
    message = \
    '-----------------------------' + '\n' + \
    '⇓ 今日起きれた人 ⇓\n' + \
    results + \
    '-----------------------------'+'\n'
    post_message_to_channel(message)

def week_results_post(table):
    ## 一週間のうち何日起きれたかを投稿
    response = table.scan()
    items = response['Items']
    results = ''
    week_check_list = [(item['name'], item['week_check']) for item in items]
    week_check_list = sorted(week_check_list, key=lambda x:x[1], reverse=True)
    for name, week_check in week_check_list:
        results += name + ' => ' + str(week_check) + '\n'
    message = \
    '-----------------------------' + '\n' + \
    '⇓ 今週の集計 ⇓\n' + \
    results + \
    '-----------------------------'+'\n'
    post_message_to_channel(message)

def interim_results_post(table):
    ## 途中経過を投稿
    response = table.scan()
    items = response['Items']
    results = ''
    week_check_list = [(item['name'], item['week_check']) for item in items]
    week_check_list = sorted(week_check_list, key=lambda x:x[1], reverse=True)
    for name, week_check in week_check_list:
        results += name + ' => ' + str(week_check) + '\n'
    message = \
    '-----------------------------' + '\n' + \
    '⇓ 途中経過です。 ⇓\n' + \
    results + \
    '-----------------------------'+'\n'
    post_message_to_channel(message)
