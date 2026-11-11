# Credentials are read from the .env file (not stored in git).
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

b2s_user = os.getenv('b2s_user')
b2s_pass = os.getenv('b2s_pass')
ofm_user = os.getenv('ofm_user')
ofm_pass = os.getenv('ofm_pass')
ssp_user = os.getenv('ssp_user')
ssp_pass = os.getenv('ssp_pass')
ssp_user2 = os.getenv('ssp_user2')
ssp_pass2 = os.getenv('ssp_pass2')
pwb_url = os.getenv('pwb_url')
pwb_user = os.getenv('pwb_user')
pwb_pass = os.getenv('pwb_pass')
chg_url = os.getenv('chg_url')
chg_user = os.getenv('chg_user')
chg_pass = os.getenv('chg_pass')
sspwds_server = os.getenv('sspwds_server')
sspwds_user = os.getenv('sspwds_user')
sspwds_pass = os.getenv('sspwds_pass')
b2s_master = os.getenv('b2s_master')
b2s_master_bar = os.getenv('b2s_master_bar')
