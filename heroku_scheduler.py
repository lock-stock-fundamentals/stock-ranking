import datetime, time
from apscheduler.schedulers.blocking import BlockingScheduler
from ranking_part_to_G import RankingClass as scf
from ranking_part_to_G_2 import RankingClass as scf_2
# from parse_insiders_1 import InsidersDeals

shed = BlockingScheduler(timezone="Europe/Moscow")

def job_function_1():
    scf().spreadsheet_forming()
    time.sleep(10)
    scf_2().PerformAll() # perform the 2-nd part right after the first one
      
shed.add_job(job_function_1, 'cron', day_of_week='tue', hour=9, minute=59)  # sun (Sunday night : 4 ; 5), make sure especially to avoid simultaneous start with tickers getter!
# shed.add_job(job_function_2, 'cron', day_of_week='sun', hour=10, minute=5)  # sun (Sunday morning (say, 10 AM)
# shed.add_job(job_function_3, 'cron', day_of_week='mon', hour=16, minute=37)  # sun (Monday morning)
shed.start()
