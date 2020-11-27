import boto3
import json
import os
from functools import reduce
import pprint
from prettytable import PrettyTable
from youtube_transcript_api import YouTubeTranscriptApi
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from google.oauth2.credentials import Credentials

comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')

def getBestVideoList(keywords):
    scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "0"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "./google_credentials/client_secret_754636752811-rmth1g8e3dl144jda8fddh1ihhj413um.apps.googleusercontent.com.json"

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)

    credentials =Credentials(
        None,
        refresh_token="1//0fNppFYz3o7ABCgYIARAAGA8SNwF-L9IrgIZJAKCn9iSH_172SxyT6cA3mMHDlSQ0MTj9MmKTc6zZRnSy1nwMW5kRkl52JYb4jhg",
        token_uri="https://accounts.google.com/o/oauth2/token",
        client_id="754636752811-rmth1g8e3dl144jda8fddh1ihhj413um.apps.googleusercontent.com",
        client_secret="KhUufHmhS8XI0srgpP__cTCr"
    )

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.search().list(
        part="snippet",
        maxResults=10,
        q=keywords,
        relevanceLanguage='en'
    )
    response = request.execute()
    return response['items']

"""     best_content_list = []
    with os.scandir('./data') as entries:
        for entry in entries:
            with open('./data/' + entry.name, 'r') as file:
                best_content_list.append(file.read())
    return best_content_list """

def getKeyPhrases(content_list, top):
    def insertFrequency(keyPhrase):
        frequency = {'overall': 0, 'min': 0, 'max': 0, 'frequency_by_content': []}
        for content in content_list:
            _freq = content.count(keyPhrase['Text'])
            frequency['overall'] += _freq
            frequency['min'] = _freq if frequency['min'] == 0 else min(frequency['min'], _freq)
            frequency['max'] = max(frequency['max'], _freq)
            frequency['frequency_by_content'].append(_freq)
        return {**keyPhrase, 'frequency': frequency}

    def filterPhrases(phrasesByContent, top):
        phrases = reduce(lambda x,y: x+y, phrasesByContent)
        phrases = list(filter(lambda x: x['Score'] >= 0.95, phrases))

        # remove duplicates
        unique_phrases=[]
        phrase_set = set()
        for phrase in phrases:
            if phrase['Text'] not in phrase_set:
                phrase_set.add(phrase['Text'])
                unique_phrases.append(phrase)

        # add frequency for each phrase
        unique_phrases = list(map(insertFrequency, unique_phrases))
        phrases = list(map(insertFrequency, unique_phrases))

        # sort key phrases by overall frequency
        phrases.sort(reverse=True, key=lambda x: x['frequency']['overall'])

        # return most frequent top phrases
        return phrases[0:top]

    # comprehend api requires each content to be less than 5000 bytes
    # see https://docs.aws.amazon.com/comprehend/latest/dg/guidelines-and-limits.html
    content_list_splited = []
    for content in content_list:
        content_list_splited.extend([content[i: i+4000] for i in range(0, len(content), 4000)])
    """ for content in content_list_splited:
        print(len(content))
 """
    def chunks(l, n):
        for i in range(0, len(l), n):
         yield l[i:i + n]

    content_list_splited_chunks = list(chunks(content_list_splited, 25))

    keyPhrasesByContent = []
    entitiesByContent = []
    for chunk in content_list_splited_chunks:
        keyPhrasesResponse = comprehend.batch_detect_key_phrases(TextList=chunk, LanguageCode='en')
        entitiesResponse = comprehend.batch_detect_entities(TextList=chunk, LanguageCode='en')

        keyPhrasesByContent.extend(list(map(lambda x: x['KeyPhrases'], keyPhrasesResponse['ResultList'])))
        entitiesByContent.extend(list(map(lambda x: x['Entities'], entitiesResponse['ResultList'])))

    return filterPhrases(keyPhrasesByContent, top), filterPhrases(entitiesByContent, top)

def getTranscript(video_id):
    print('Getting transcript for '+video_id)
    return YouTubeTranscriptApi.get_transcript(video_id)

def printTable(phrases):
    table = PrettyTable()
    table.field_names=['phrase', 'score', 'overall_freq', 'min_freq', 'max_freq', 'entity_type']
    for phrase in phrases:
        entity_type = 'N/A'
        if 'Type' in phrase:
            entity_type = phrase['Type']
        table.add_row([phrase['Text'], phrase['Score'], phrase['frequency']['overall'], phrase['frequency']['min'], phrase['frequency']['max'], entity_type])
    print(table)

# best_content_list = getBestList()
# key_phrases = getKeyPhrases(best_content_list)

# transcript = YouTubeTranscriptApi.get_transcript("RDjlhhbbPJE")


keywords = 'How To Buy Your First Rental'
print('Working on keywords: ' + keywords)
video_list = getBestVideoList(keywords)
# video_list = [{'id': {'videoId': 'kqMtDrsc5Pw'}}]
transcript_list = []
for video in video_list:
    try: 
        transcript_list.append(getTranscript(video['id']['videoId']))
    except:
        pass

content_list = []
for transcript in transcript_list:
    content = '. '.join(list(map(lambda x: x['text'], transcript)))
    # print(content)
    # print("\n\n")
    content_list.append(content)

keyPhrases, entities = getKeyPhrases(content_list, 100)

# print top keyphrases table
printTable(keyPhrases)
printTable(entities)
