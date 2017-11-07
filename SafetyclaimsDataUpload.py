#!/apollo/sbin/envroot "$ENVROOT/bin/python2.7"
import pandas as pd
import boto
import boto3
import boto.s3.connection
import os, sys
import csv
import smtplib
import xlrd
import sys
import datetime as dt
from sqlalchemy import create_engine
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat
from datetime import datetime
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import pyodinhttp




def encodest(code):
    # print claim,code.encode('utf-8')
    return code.encode('utf-8').strip()

def whitespace(x):
    if type(x) is str:
        return x.strip()
    else:
        return x




def Main():
    try:
        download_loc = r'/var/tmp/OSS/ARCClaims.xlsx'
        # download_loc = 'C:\WorkSpace\Data\ARC\ARCClaims.xlsx'

        # ***********************************OSS connection****************************************************#
        # OSS warehouse data
        #fix with comma

        linehaul_user=pyodinhttp.odin_retrieve('com.amazon.ats-atlas.keys.OSS', 'Principal').data

        linehaul_pass=pyodinhttp.odin_retrieve('com.amazon.ats-atlas.keys.OSS', 'Credential').data
        linehaul_contr='ats-linehaul-dw.cwfqj3baiwdp.us-east-1.redshift.amazonaws.com'
        linehaul_port=8192
        linehaul_dbname='atslinehauldb'



        engine_string = "postgresql+pygresql://%s:%s@%s:%d/%s" % (linehaul_user, linehaul_pass, linehaul_contr, linehaul_port, linehaul_dbname)


        engine_oss = create_engine(engine_string)


        # ***********************************Download file from S3****************************************************#
        access_key = pyodinhttp.odin_retrieve('com.amazon.ats-atlas.keys.AWSOSS1', 'Principal').data
        secret_key = pyodinhttp.odin_retrieve('com.amazon.ats-atlas.keys.AWSOSS1', 'Credential').data
        connS3 = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key, calling_format=OrdinaryCallingFormat())
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        path='ARCClaims/FilesFromCarrier/'
        bucket_name = 'ats-purchased'



        bucket = connS3.get_bucket(bucket_name)
        bucket_list = bucket.list()
        currentdate = str(dt.date.today())
        for filename in bucket_list:
            if filename.key == 'ARCClaims/FilesFromCarrier/Arc-' + currentdate + '.xlsx': #Nikki-Aug032017_05_08_29.xlsx
                key = bucket.get_key(filename)
                key.get_contents_to_filename(r'/var/tmp/OSS/Arc-' + currentdate + '.xlsx')

        print 'download done'
        importxl = pd.read_excel(r'/var/tmp/OSS/Arc-' + currentdate + '.xlsx', header=0)

        dfimport=pd.DataFrame(importxl)

        nonamz = 'U'
# ***********************************DATA PREPARATION****************************************************#
        print 'Data Manipulation'
        dfimport['DOL'] = dfimport.apply(lambda x: '1/1/1900' if pd.isnull(x['DOL']) else x['DOL'], axis=1)
        dfimport['TIME OF LOSS'] = dfimport.apply(lambda x: '00:00:00' if pd.isnull(x['TIME OF LOSS']) else x['TIME OF LOSS'], axis=1)
        dfimport['REPORTED TO ARC DATE'] = dfimport.apply(lambda x: '1/1/1900' if pd.isnull(x['REPORTED TO ARC DATE']) else x['REPORTED TO ARC DATE'], axis=1)
        dfimport['REPORTED TO ARC TIME'] = dfimport.apply(lambda x: '00:00:00' if pd.isnull(x['REPORTED TO ARC TIME']) else x['REPORTED TO ARC TIME'], axis=1)
        dfimport['AMAZONTRAILER']=dfimport['AMAZON TRAILER'].astype(str)
        dfimport['NONAMAZONTRAILER'] = dfimport['NON-AMAZON TRAILER'].astype(str)
        dfimport['AmzTrailer1'] = dfimport.AMAZONTRAILER.str.split(',', expand=True)[0]
        dfimport['AmzTrailer2'] = dfimport.AMAZONTRAILER.str.split(',', expand=True)[1]
        dfimport['EMA CONTACT'].astype(str)
        dfimport['POLICE REPORT#'].astype(str)


        dfimport.fillna(value='', inplace=True)
        # dfimport.NONAMAZONTRAILER.fillna(value='none', inplace=True)
        # dfimport.NONAMAZONTRAILER.fillna(value='', inplace=True)
        dfimport['NONAMAZONTRAILER'] = dfimport.apply(lambda x: '' if (x['NONAMAZONTRAILER'] == 'nan') else x['NONAMAZONTRAILER'], axis=1)


    #***********************************Create new Dataframe with table columns****************************************************#
        columns=['claim_nbr','loss_dt','report_dt','loss_loc','loss_state','loss_reg','vrid','origin','destination','otr_f','carrier','carrier_contact','program','tractor_id','tractor_damage_f'
              ,'driver_name','trailer1_id','trailer2_id','amzn_trailer_f','branded_f','trailer_damage_f','loaded_f','load_damage','accident_details','custody_of_care','emrg_mgmt_agency'
              ,'emrg_contact','police_rep_f','police_rep_recv_f','police_rep_id','pictures_scene_f','pictures_damage_f','amzn_loc','claimant_last_name','inside_veh_maneuver','loss_descr'
              ,'preventable_f','dispos_cargo_f','dispos_trailer_f','create_ts','last_update_ts']

        df = pd.DataFrame(columns=columns)

        df['claim_nbr'] = dfimport['CLAIM NUMBER'].astype(str)
        df['loss_dt']   = dfimport[['DOL', 'TIME OF LOSS']].apply(lambda x: ' '.join(x), axis=1)
        df['loss_dt']   =pd.to_datetime(df['loss_dt'])
        df['loss_dt']   =df['loss_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['report_dt'] = dfimport[['REPORTED TO ARC DATE', 'REPORTED TO ARC TIME']].astype(str).apply(lambda y: ' '.join(y), axis=1)
        df['report_dt'] = pd.to_datetime(df['report_dt'])
        df['report_dt'] = df['report_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['loss_loc']  = dfimport['LOSS LOCATION'].astype(str)
        df['loss_state']= dfimport['LOSS STATE OR PROVINCE'].astype(str)
        df['loss_reg']  = dfimport['LOSS REGION'].astype(str)
        df['vrid']      =  dfimport['VRID'].astype(str)
        dfimport['ORIGIN']    = dfimport['ORIGIN'].astype(str)
        df['origin']=dfimport.apply(lambda x: encodest(x['ORIGIN']), axis=1)
        dfimport['DESTINATION']= dfimport['DESTINATION'].astype(str)
        df['destination'] = dfimport.apply(lambda x: encodest(x['DESTINATION']), axis=1)
        df['otr_f']           = dfimport.apply(lambda x: 1 if x['OVER THE ROAD/IN YARD'] == 'Over the Road' else 0, axis=1)
        df['carrier']         = dfimport['CARRIER'].astype(str)
        df['carrier_contact'] = dfimport['CARRIER CONTACT INFO'].astype(str)
        df['program']         = dfimport['CENTER'].astype(str)
        df['tractor_id']      = dfimport['CARRIER TRACTOR #'].astype(str)
        df['tractor_damage_f']= dfimport.apply(lambda x: 1 if x['TRACTOR DAMAGE'] == 'Y' else 0, axis=1)
        df['driver_name']     = dfimport['DRIVER NAME']
        # print dfimport['NONAMAZONTRAILER']
        df['trailer1_id'] = dfimport.apply(
            lambda x: x['AmzTrailer1'] if (x['AmzTrailer1'] != '' or x['AmzTrailer1'] != 'None') else x[
                'NONAMAZONTRAILER'], axis=1)
        df['trailer2_id'] = dfimport.apply(
            lambda x: x['AmzTrailer2'] if (x['AmzTrailer2'] != '' or x['AmzTrailer2'] != 'None')  else '', axis=1)
        df['trailer1_id'] = dfimport.apply(
            lambda x: x['NONAMAZONTRAILER'] if (
            (x['AmzTrailer1'] == '' or x['AmzTrailer1'] == 'None') and x['NONAMAZONTRAILER'] != '') else x[
                'AmzTrailer1'], axis=1)
        df['amzn_trailer_f'] = dfimport.apply(
            lambda x: 0 if (x['AmzTrailer1'] == '' or x['AmzTrailer1'] == 'None') else 1, axis=1)


        df.trailer1_id.to_string().replace('nan', '')
        df['branded_f']       = dfimport.apply(lambda x: 1 if x['BRANDED(Y/N)'] == 'Y' else 0, axis=1)
        df['trailer_damage_f']= dfimport.apply(lambda x: 1 if x['TRAILER DAMAGE'] == 'Y' else 0, axis=1)
        df['loaded_f']        = dfimport.apply(lambda x: 1 if x['CARGO ON BOARD?'] == 'Y' else 0, axis=1)

        df['load_damage']     = dfimport.apply(lambda x:  encodest(x['CARGO DAMAGE']), axis=1)
        df['accident_details']= dfimport.apply(lambda x: encodest(x['ACCIDENT DETAILS']),axis=1)
        df['custody_of_care'] = dfimport.apply(lambda x: encodest(x['CUSTODY OF CARE']),axis=1)
        df['emrg_mgmt_agency']= dfimport.apply(lambda x: encodest(x['EMERGENCY MANAGEMENT AGENCY']), axis=1)


        df['emrg_contact'] = dfimport['EMA CONTACT']




        df['police_rep_f'] = dfimport.apply(lambda x: 1 if x['POLICE REPORT FILED'] == 'Yes' else 0, axis=1)

        df['police_rep_recv_f'] = dfimport.apply(lambda x: 1 if x['POLICE REPORT OBTAINED'] == 'Yes' else 0,axis=1)

        df['police_rep_id'] = dfimport['POLICE REPORT#']

        df['pictures_scene_f'] = dfimport.apply(lambda x: 1 if x['PICTURES OF SCENE'] == 'Yes' else 0, axis=1)
        df['pictures_damage_f'] = dfimport.apply(lambda x: 1 if x['ADDITIONAL PICTURES OF DAMAGE'] == 'Yes' else 0, axis=1)

        df['amzn_loc'] = dfimport.apply(lambda x: encodest(x['STATION OR ROUTE']), axis=1)
        df['claimant_last_name'] = dfimport.apply(lambda x: encodest(x['CLAIMANT LAST NAME']), axis=1)
        df['inside_veh_maneuver'] = dfimport.apply(lambda x: encodest(x['INSD VEH MANEUVER']), axis=1)
        df['loss_descr'] = dfimport.apply(lambda x: encodest(x['LOSS DESCRIPTION']), axis=1)


        # df['loss_descr']=df.loss_descr.str.rstrip(' ')





        df['preventable_f'] = dfimport.apply(lambda x: 1 if x['PREVENTABLE/NON-PREVENTABLE'] == 'P' else 0,axis=1)
        df['dispos_cargo_f'] = dfimport.apply(
            lambda x: 1 if x['DISPOSITION OF AMAZON CARGO CONFIRMED'] == 'Y' else 0, axis=1)
        df['dispos_trailer_f'] = dfimport.apply(
            lambda x: 1 if x['DISPOSITION OF AMAZON TRAILER CONFIRMED'] == 'Y' else 0, axis=1)


        df['create_ts'] = dt.datetime.now()
        df['last_update_ts'] = dt.datetime.now()

        df['create_ts'] = df['create_ts'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['last_update_ts'] =df['last_update_ts'].dt.strftime('%Y-%m-%d %H:%M:%S')


        df.columns.str.strip()

        print 'Removing Whitespace'

        df.applymap(whitespace)

        #***********************************save df to csv OSS****************************************************#

        print 'Saving outputfile'

        df.to_csv(r'/var/tmp/OSS/ARCOutputData-'+currentdate+'.csv',index=False)
        # ***********************************Load output to s3****************************************************#


        outputfile = r'/var/tmp/OSS/ARCOutputData-'+currentdate+'.csv'
        # outputfile = r'C:\WorkSpace\Data\ARC\ARCOutputData.csv'
        print 'Uploading %s to Amazon S3 bucket %s' % \
              (outputfile, bucket_name)

        def percent_cb(complete, total):
            sys.stdout.write('.')
            sys.stdout.flush()

        bucket_name = 'ats-purchased//ARCClaims//FilesFromAmazon'
        # key_name = 'ats-purchased/ARCClaims/FilesFromCarrier/Nikki_aug032017_05_08_29.xlsx'
        bucket = connS3.get_bucket(bucket_name)

        k = Key(bucket)
        k.key = 'ARCOutputData-'+currentdate+'.csv'
        k.set_contents_from_filename(outputfile,cb=percent_cb, num_cb=10)
        # ***********************************S3 to Redshift****************************************************#
        print 'Copying s3 to Redshift'
        table='oss_stg.safety_claims_raw'
        fn='s3://ats-purchased/ARCClaims/FilesFromAmazon/ARCOutputData-'+currentdate+'.csv'

        conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(linehaul_dbname,linehaul_port,linehaul_user, linehaul_pass, linehaul_contr)
        # connect to Redshift (database should be open to the world)

        con = engine_oss.connect()
        sql ="""truncate table oss_stg.safety_claims_raw;
              COPY %s FROM '%s'
              CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
              FORMAT CSV
              IGNOREHEADER AS 1
              ACCEPTINVCHARS AS '-'
              BLANKSASNULL
              EMPTYASNULL
              TRIMBLANKS
               ; commit;""" %(table, fn, access_key, secret_key)

        merge="""begin transaction;
        delete from original.safety_claims 
        using oss_stg.safety_claims_raw 
        where original.safety_claims .claim_nbr = oss_stg.safety_claims_raw .claim_nbr; 
        
        insert into original.safety_claims  
        select * from oss_stg.safety_claims_raw;
        
        end transaction;"""

        # cur=con.cursor()
        con.execute(sql)
        print 'OSS table merge'
        con.execute(merge)
        # con.close()

        print 'Complete!'


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








def send_mail(send_from,send_to,subject,text,server,port,username='',password='',isTls=True):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    print 'Email Processing'

    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect('smtp.amazon.com')
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()




if __name__ == '__main__':
    pd.set_option('display.width', 500)
    Main()