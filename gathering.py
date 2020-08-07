import os
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from PIL import Image
import urllib.request
import pandas as pd
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import lxml

CSV_DIR = '/home/ubuntu/csv/gathering/'
IMG_DIR = '/home/ubuntu/images/gathering/'

def compare_seq(csv_seq,seq):
	#print('csv_seq: {}, seq: {}'.format(csv_seq,seq))
	if csv_seq == seq:
		return 1
	else:
		return 0

def make_seq(href):
	tmp = []
	tmp = href.split('/')
	tmp2 = tmp[len(tmp)-1]

	return tmp2		

def identified():
	datetime.today()
	tid = datetime.today().strftime("%Y%m%d%H%M")

	return tid

def write_csv(df):

	savename = CSV_DIR+'fishing.csv'

	tmp = []
	tmp = savename.split('/')

	tmp2 = tmp[len(tmp)-1]

	if os.path.exists(savename):
		print('Add data\n', tmp2)

		df_read = pd.read_csv(savename, header=None)

		last_row = df_read.tail(1)
		csv_seq = last_row.iloc[:,0]
		result = compare_seq(int(csv_seq.values[0]),int(df['seq'].values[0]))
	else :
  		print('Make file\n', tmp2)	
  		result = 0

	if result:
		print('overlap contents!!!')
	else:
		df.to_csv(savename, header=False, index=False, mode='a', encoding='utf-8-sig')	

	return result	

def make_csv(df):

	savename = CSV_DIR+'fishing.csv'

	tmp = []
	tmp = savename.split('/')

	tmp2 = tmp[len(tmp)-1]

	if os.path.exists(savename):
		print('Add data ', tmp2)
	else :
  		print('Make file ', tmp2)	

	df.to_csv(savename, header=False, index=False, mode='a', encoding='utf-8-sig')	

	upload_googledrive(savename, 'csv')

def download_image(tid,image):

	tmp = []
	tmp = image.split('/')

	tmp2 = tmp[len(tmp)-1]
	exe = tmp2.split('.')

	name = tid+'.'+exe[1]

	final = name.split('?')

	savename = IMG_DIR+final[0]

	urllib.request.urlretrieve(image, savename)

	with Image.open(IMG_DIR+final[0]) as image:
		image.save(IMG_DIR+final[0], quality=40)	

	upload_googledrive(savename, 'jpeg')

def scrappy(soup):

	seq, store, area, manage, kind, src = [], [], [], [], [], []

	view = soup.select('a.talk_view_btn')

	try:
		seq.append(make_seq(str(view[0].get('href'))))
	except ValueError:
		seq.append('seq')

	profile = soup.select('div.profile_line div.profile_name')

	try:
		store.append(str(profile[0].select('strong')[0].text).replace(' ',''))
	except ValueError:
		store.append('store')
	
	try:
		area.append(str(profile[0].select('p')[0].text).replace(' ',''))
	except ValueError:
		area.append('area') 

	talk = soup.select('p.talk_pic')

	try:
		manage.append(str(talk[0].select('span.manage')[0].text).replace(' ',''))
	except ValueError:
		manage.append('manage')

	try:
		kind.append(str(talk[0].select('span.kind')[0].text).replace(' ',''))
	except ValueError:
		kind.append('kind')

	image = soup.select('div.img_box img')
	src.append(image[0]['src'])
	# print(store, area, manage, kind, src)
	dic_fishing = {}

	dic_fishing['seq'] = seq
	dic_fishing['store'] = store
	dic_fishing['area'] = area
	dic_fishing['manage'] = manage
	dic_fishing['kind'] = kind
	dic_fishing['img'] = identified()	
	dic_fishing['src'] = src

	df_fishing = pd.DataFrame(dic_fishing)

	#make_csv(df_fishing)
	#download_image(identified(),src[0])

	result = write_csv(df_fishing)

	if result:
		print('overlap contents !!!')
	else:
		download_image(identified(),src[0])

def upload_googledrive(savename, kind):

	if kind == 'jpeg':
		minetype = 'image/jpeg'
	elif kind == 'csv':
		minetype = 'text/csv'
	else:
		mintype = 'text/html'

	try :
		import argparse
		flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
	except ImportError:
		flags = None

	SCOPES = 'https://www.googleapis.com/auth/drive.file'
	store = file.Storage('/home/ubuntu/datagathering/storage.json')
	creds = store.get()

	if not creds or creds.invalid:
		print("make new storage data file ")
		flow = client.flow_from_clientsecrets('/home/ubuntu/datagathering/credentials.json', SCOPES)
		creds = tools.run_flow(flow, store, flags) if flags else tools.run(flow, store)

	DRIVE = build('drive', 'v3', http=creds.authorize(Http()))

	FILES = (
    	(savename),
	)

	for file_title in FILES :
		file_name = []
		file_name = file_title.split('/')
		metadata = {
    				'name': file_name[-1],
                	'mimeType': minetype,
                	'parents': ['GOOGLE_DRIVE_FOLER_SHARE_ID']
                	}
		
		res = DRIVE.files().create(body=metadata, media_body=file_title).execute()

		if res:
			result = 'Uploaded {}'.format(file_name[-1])
			print(result)

url = 'https://www.moolban.com/talk/list_more?'

params = {
    'talk_key':6
}

resp = requests.get(url, params)
json_data = json.loads(resp.text) 
#print(json.dumps(json_data, indent=4))

soup = BeautifulSoup(json_data['html'], 'lxml')

scrappy(soup)
