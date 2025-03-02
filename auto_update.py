

from finance_calendars import finance_calendars as fc
from datetime import datetime, timedelta
import pandas as pd

from dotenv import load_dotenv
import os

import asyncio
from dida365 import Dida365Client, ServiceType, TaskCreate, ProjectCreate, TaskPriority, TaskUpdate

import schedule
import time

from futu import *

load_dotenv()


# client_id = os.getenv('CLIENT_ID')
# client_secret = os.getenv('CLIENT_SECRET')

# 存放投资事件的清单id
invest_project_id = os.getenv('INVEST_PROJECT_ID') 

def get_next_n(n):
    # exclude today
    import datetime
    today = datetime.date.today()
    next_n_days = [today + datetime.timedelta(days=i) for i in range(1, n+1)]
    return next_n_days  

async def main():
    print('Start:', datetime.now())
    client = Dida365Client(
        service_type=ServiceType.DIDA365,  # or DIDA365
        redirect_uri="http://localhost:8080/callback",  # Optional
    )

    # First-time authentication:
    if not client.auth.token:
        # This will start a local server at the redirect_uri
        # and open your browser for authorization
        await client.authenticate()
        # Token will be automatically saved to .env if save_to_env=True
        
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ret, data = quote_ctx.get_user_security("focus")
    if ret == RET_OK:
        print(data)
        if data.shape[0] > 0:  # If the user security list is not empty
            print(data['code'][0]) # Take the first stock code
            print(data['code'].values.tolist()) # Convert to list
    else:
        print('error:', data)
    quote_ctx.close() # After using the connection, remember to close it to prevent the number of connections from running out

    stock_list = data.code.values.tolist()
    stock_list = [a.split('.')[1] for a in stock_list]
    
    # 获取已有任务
    now_tasks = await client.get_project_with_data(project_id=invest_project_id)    
    now_tasks = [task.title for task in now_tasks.tasks]        
    
    dates = get_next_n(7)
    
    # 宏观事件
    for date in dates:
        try:
            macro_df = ak.macro_info_ws(date=datetime.strftime(date, '%Y%m%d')) 
        except:
            continue 
        important_events = macro_df[(macro_df['重要性'] == 3) & (macro_df['地区'] == '美国')]
        for i in range(important_events.shape[0]):
            name = '美国' + important_events.iloc[i]['事件']
            name_with_tag = name + ' #日历'
            time = important_events.iloc[i]['时间']
            if name not in now_tasks:
                task = await client.create_task(
                    TaskCreate(
                        project_id=invest_project_id,  # Required: tasks must belong to a project
                        title=name_with_tag,
                        priority=TaskPriority.NONE,  # Enum: NONE, LOW, MEDIUM, HIGH
                        start_date=datetime.strptime(time, '%Y-%m-%d %H:%M:%S'),
                        reminders=['TRIGGER:PT0S'],
                        is_all_day=False,
                        time_zone="UTC+08:00"
                    )
                )
                updated_task = await client.update_task(
                    TaskUpdate(
                        id=task.id,
                        project_id=task.project_id,  # Both id and project_id are required
                        title=name,
                    )
                )
    
    # 自选股业绩
    for date in dates:
        earnings = fc.get_earnings_by_date(date).reset_index()
        for i in range(len(earnings)):
            symbol = earnings.loc[i]['symbol']
            time = earnings.loc[i]['time']
            if symbol in stock_list:
                print(symbol, date, time)
                name = symbol + '业绩'
                name_with_tag = name + ' #日历'
                if name not in now_tasks:
                    if time == 'time-not-supplied':
                        task = await client.create_task(
                            TaskCreate(
                                project_id=invest_project_id,  # Required: tasks must belong to a project
                                title=name_with_tag,
                                priority=TaskPriority.NONE,  # Enum: NONE, LOW, MEDIUM, HIGH
                                start_date=date,
                                is_all_day=True,
                                time_zone="UTC+08:00"
                            )
                        )
                    elif time == 'time-after-hours':
                        next_day = date + timedelta(days=1)
                        after_date = datetime(next_day.year, next_day.month, next_day.day, 5, 0, 0)
                        task = await client.create_task(
                            TaskCreate(
                                project_id=invest_project_id,  # Required: tasks must belong to a project
                                title=name_with_tag,
                                priority=TaskPriority.NONE,  # Enum: NONE, LOW, MEDIUM, HIGH
                                start_date=after_date,
                                reminders=['TRIGGER:PT0S'],
                                is_all_day=False,
                                time_zone="UTC+08:00"
                            )
                        )
                    elif time == 'time-pre-market':
                        pre_date = datetime(date.year, date.month, date.day, 21, 30, 0)
                        task = await client.create_task(
                            TaskCreate(
                                project_id=invest_project_id,  # Required: tasks must belong to a project
                                title=name_with_tag,
                                priority=TaskPriority.NONE,  # Enum: NONE, LOW, MEDIUM, HIGH
                                start_date=pre_date,
                                reminders=['TRIGGER:PT0S'],
                                is_all_day=False,
                                time_zone="UTC+08:00"
                            )
                        )
                    updated_task = await client.update_task(
                        TaskUpdate(
                            id=task.id,
                            project_id=task.project_id,  # Both id and project_id are required
                            title=name,
                        )
                    )
    print('End:', datetime.now())
    
if __name__ == "__main__":
    def job():
        asyncio.run(main()) 
    
    # 每周日的某个时间运行
    schedule.every().sunday.at("00:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)