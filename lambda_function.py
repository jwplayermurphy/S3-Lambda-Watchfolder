import boto3
import botocore
import jwplatform
import requests
import json
import csv
import urllib3

# This function is a proof of concept and should not be used for production
# Never share your API Key or Secret. These can not be regenerated
# This script assumes metadata & captions always precede video content in the dropfolder

def lambda_handler(event, context):
    #define S3 & AWS params
    s3 = boto3.resource('s3')
    s3Client = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    file_path = key.split(".")[0]
    file_type = key.split(".")[1]
    bucket_url = f"https://s3.us-east-2.amazonaws.com/{bucket_name}/"
    
    # Key & secret for JW Platform API calls
    # This would be dynamic in a multi-tenant scenario
    vkey = ''
    vsecret = ''
        
    # Open our CSV file
    # Put data into a list
    def CSVtoJSON(csv_file):
        # Open our CSV file
        with requests.Session() as s:
            download = s.get(csv_file)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            return list(cr)

    def checkForFile(file, bucket):
        # try to hit this the file with a HEAD request
        # should check for any captions or metadata tracks too
        try:
            s3.Object(bucket, file).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                
                return False
                
            else:
                
                return False
        else:
            return True
    
    def publicUrl(file):
        
        pub_file = s3Client.generate_presigned_url('get_object', Params = {'Bucket': bucket_name, 'Key': file}, ExpiresIn = 1000)    
        return pub_file
    
    def uploadCaptions(api_response, captions_file_path, label):
        captions_obj = {}
        captions_obj['video_key'] = api_response.get('video',{'key': 'NA'})['key']
        captions_obj['kind'] = "subtitles"
        captions_obj['label'] = label
        response = jwplatform_client.videos.tracks.create(**captions_obj)
        
        # Construct base url for upload
        upload_url = '{}://{}{}'.format(
            response['link']['protocol'],
            response['link']['address'],
            response['link']['path']
        )
    
        # Query parameters for the upload
        query_parameters = response['link']['query']
        query_parameters['api_format'] = "json"
        
        http = urllib3.PoolManager()

        url = publicUrl(captions_file_path)
        response = http.request('GET', url)
        f = response.data
        
        files = {'file': f}
        r = requests.post(upload_url, params=query_parameters, files=files)
        
        return r
    
    # check if the file is an mp4 or not
    # This should check for all filetypes we support on ingest
    if file_type == "mp4" or file_type == "mov":
        
        # need to add a check to ensure the csv exists
        # parse the CSV into a List
        csv_url = publicUrl("key.csv")
        data_file_path = CSVtoJSON(csv_url)
        
    else:
        
        exit()

    # we only want to execute the script for video files
    # if not a video file, we exit the script    
    
    ###### begin script execution ######
    
    # Define JW Player
    jwplatform_client = jwplatform.Client(vkey, vsecret)
    
    # Parse data from CSV
    # Ignore the header row
    i = 0
    table_dict = {}
    media_object = {}

    # Ignore the header row
    while i < len(data_file_path):
        if i == 0:
            for index,value in enumerate(data_file_path[i]):
                table_dict[index] = value
            i += 1
        else:
            if data_file_path[i][14] == key:
                for index, value in enumerate(data_file_path[i]):
                    # Ingest all metadata as custom parameters
                    customParamName = "custom." + table_dict[index]
                    media_object[customParamName] = value
            
            i += 1
    
    # Append our download URL to the file to the object                        
    media_object['title'] = media_object['custom.Title']
    media_object['description'] = media_object['custom.Summary_Short']
    media_object['download_url'] = publicUrl(key)
    media_object['upload_method'] = 's3'
    
    if media_object["custom.Captions_file_Name"] and media_object["custom.Captions_file_Name"] != "":
            
        captions_file_path = media_object["custom.Captions_file_Name"]
        
    # Make the JW Player API call and create the asset
    api_response = jwplatform_client.videos.create(**media_object)
            
    if captions_file_path:
       uploadCaptions(api_response, captions_file_path, "English")
    
    return api_response