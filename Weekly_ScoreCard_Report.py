#!/apollo/sbin/envroot "$ENVROOT/bin/python2.7"
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 08:20:58 2016

@author: arcsiva
"""

import os, sys
import pandas as pd
import boto
import boto3
import boto.s3.connection
import datetime as dt
from boto.s3.connection import OrdinaryCallingFormat
import pyodinhttp



from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import smtplib

def Main():
    try:
        access_key = pyodinhttp.odin_retrieve('com.amazon.ats-atlas.keys.AWSOSS1', 'Principal').data
        secret_key = pyodinhttp.odin_retrieve('com.amazon.ats-atlas.keys.AWSOSS1', 'Credential').data
        # ************************************************************************************************************#
        # ***********************************Download file from S3****************************************************#
        # ************************************************************************************************************#
        connS3 = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                 calling_format=OrdinaryCallingFormat())
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        path = 'CarrierFiles/Documents/'
        bucket_name = 'ats-purchased'
        bucket = connS3.get_bucket(bucket_name)
        bucket_list = bucket.list()
        currentdate = str(dt.date.today())
        for filename in bucket_list:
            if filename.key == 'CarrierFiles/Documents/SCAC_List.xlsx':
                key = bucket.get_key(filename)
                key.get_contents_to_filename(r'/var/tmp/OSS/SCAC_List.xlsx')

        print 'download done'
        importxl = pd.read_excel(r'/var/tmp/OSS/SCAC_List.xlsx', header=0)

        dfScac = pd.DataFrame(importxl)
        # ************************************************************************************************************#
        # ***************************************Upload PDF to Carrier folder in S3***********************************#
        # ************************************************************************************************************#
        for i in range(0, len(dfScac)):
            # Build path
            Carriername = dfScac['Scac_Name'][i]
            pdfname = "Carrier_Performance_" + dfScac['Scac_Code'][i] + ".pdf"
            path = Carriername + '/FilesFromAmazon/'
            full_key_name = os.path.join(path, pdfname)
            # Set and save in S3
            k = bucket.new_key(full_key_name)
            local_path = r'/var/tmp/'
            k.set_contents_from_filename(local_path + pdfname)


    except:
        print "Error!"

        exc_type, exc_value, exc_traceback = sys.exc_info()


        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'lineno': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': exc_type.__name__,
            'message': exc_value.message,  # or see traceback._some_str()
        }
        print traceback_details

        print exc_type, exc_value, exc_traceback,exc_traceback.tb_lineno
        #Email
        text = traceback_details
        server = 'smtp.amazon.com'
        port = 8192
        send_mail(extype=exc_type,exvalue=exc_type,extrace=exc_traceback,exline=exc_traceback.tb_lineno)


def send_mail(send_from='ATS_BI@amazon.com', send_to='arcsiva@amazon.com', subject='Weekly_ScoreCard_Report-ERROR', isTls=True,extype='',exvalue='',extrace='',exline=''):

    from email.utils import formatdate
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    text = extype+exvalue+extrace+exline
    html = """    <html>
                     <head></head>
                     <body>
                       <p><br>
                           <br>

                        Weekly score card report python code errored

                       <br>
                       Archana Sivakumar
                       <br>
                       <br>
                       </p>
                     </body>
                   </html>
                   """.format()
    msg.attach(MIMEText(text))
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part2,part1)
    part = MIMEBase('application', "octet-stream")


    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect('smtp.amazon.com')
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
    print 'Email() done'


