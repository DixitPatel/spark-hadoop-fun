import json
import boto3
from operator import add
import re
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

NON_ALPHABETIC_REGEX = re.compile(r"[^a-zA-Z]")

STOP_WORDS = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any","are",
                  "arent", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
                  "but", "by", "cant", "cannot", "could", "couldnt", "did", "didnt", "do", "does", "doesnt",
                  "doing","dont", "down", "during", "each", "few", "for", "from", "further", "had", "hadnt",
                  "has", "hasnt","have", "havent", "having", "he", "hed", "hell", "hes", "her", "here", "heres",
                  "hers", "herself","him", "himself", "his", "how", "hows", "i", "id", "ill", "im", "ive", "if",
                  "in", "into", "is", "isnt","it", "its", "its", "itself", "lets", "me", "more", "most", "mustnt",
                  "my", "myself", "no", "nor","not", "of", "off", "on", "once", "only", "or", "other", "ought",
                  "our", "ours", "ourselves", "out","over", "own", "same", "shant", "she", "shed", "shell",
                  "shes", "should", "shouldnt", "so", "some","such", "than", "that", "thats", "the", "their",
                  "theirs", "them", "themselves", "then", "there","theres", "these", "they", "theyd", "theyll",
                  "theyre", "theyve", "this", "those", "through", "to","too", "under", "until", "up", "very",
                  "was", "wasnt", "we", "wed", "well", "were", "weve", "were","werent", "what", "whats", "when",
                  "whens", "where", "wheres", "which", "while", "who", "whos", "whom","why", "whys", "with",
                  "wont", "would", "wouldnt", "you", "youd", "youll", "youre", "youve", "your","yours",
                  "yourself", "yourselves"]


def lambda_handler(event, context):
    #print("HelloWord!")
    #print(event)
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    print(bucket_name, file_key)
    
    outputBucket="project-3-serverless-patel-sapra-output"
    
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    #wordCount = tabDelimToDict(s3, outputBucket, "output.txt")
    
    lines = obj['Body'].read().decode("utf-8").split()
    #print("previous wordCount: ",wordCount)
    
    words = []
    
    for x in lines:
        #x = x.decode("utf-8") 
        x = re.sub(NON_ALPHABETIC_REGEX, "", x)
        x = x.lower()
        
        if x in STOP_WORDS or x == "":
            continue
        
        words.append(x)
           
        #if x in wordCount:
        #    wordCount[x] += 1
        #else:
        #    wordCount[x] = 1
        
    #print("newWordCount", wordCount)
    
    #persist to S3 
    s3.put_object(Body=outputTweet(words), Bucket=outputBucket, Key=file_key)
    
    
def dictToTabDelim(inputDict):
    tab_delim = ""
    for key, value in sorted(inputDict.items(), key=lambda x: (-x[1],x[0]), reverse=False):
       tab_delim += key + "\t" + str(value) + "\n"
    #remove last newLine   
    return tab_delim[:-1]

def outputTweet(words):
    output_tweet  = ""
    for word in words:
       output_tweet += word + " "
    #remove last newLine   
    return output_tweet

def tabDelimToDict(s3, s3Bucket, s3File):
    wordFreq = {}
    
    if(check(s3, s3Bucket, s3File) == False):
        return wordFreq
    
    #wordFreq exists
    obj = s3.get_object(Bucket=s3Bucket, Key=s3File)
    
    lines = obj['Body'].read().decode('utf-8').split("\n")
    
    for line in lines:
        keyVal = line.split("\t")
        wordFreq[keyVal[0]] = int(keyVal[1])
    
    return wordFreq
    
def check(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True

