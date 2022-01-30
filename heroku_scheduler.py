import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from ranking_part_to_G import RankingClass as scf
from ranking_part_to_G_2 import RankingClass as scf_2
from w2w_total_change import RankingClass as wtc
from parse_insiders_1 import InsidersDeals

shed = BlockingScheduler(timezone="Europe/Moscow")

def job_function_1():
    scf().spreadsheet_forming()
    
def job_function_2():
    scf_2().preparing_rank_sheets()

def job_function_3():
    wtc().total_change_calc()
    
def job_function_4():
    InsidersDeals().PerformAll()

shed.add_job(job_function_1, 'cron', day_of_week='sun', hour=2, minute=5)  # sun (Sunday night : 2 ; 5)
shed.add_job(job_function_2, 'cron', day_of_week='sun', hour=15, minute=5)  # sun (Sunday morning)
shed.add_job(job_function_3, 'cron', day_of_week='mon', hour=5, minute=10)  # sun (Monday morning)
# shed.add_job(job_function_4, 'cron', day_of_week='wed', hour=16, minute=24)  # set to !!! 8 and 5, sun (Sunday)
shed.start()
