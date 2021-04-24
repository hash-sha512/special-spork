import requests
import logging
import boto3
from botocore.exceptions import ClientError

def jsonToLocalCsv(data):
    with open("unbxd-2021-interns-test-harsha.csv", "a") as f:
        for i in data:
            #delimeter used is ';' because the delimiting comma strangely interacts with the commas within strings and does a split even at these commas which are supposedly within the single valued string 
            product = ';'.join(i.values())
            f.write(f"{product}\n")
            
def uploadToS3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True    
        

def apiReshape():
    r = requests.get('https://search.unbxd.io/fb853e3332f2645fac9d71dc63e09ec1/demo-unbxd700181503576558/search?&q=*&rows=10&start=0')
    totalProducts = r.json()['response']['numberOfProducts']
    productCount = 0
    productsPerApiCall = 10
    for offset in range(1, (totalProducts//productsPerApiCall)+1):
        r = requests.get(f"https://search.unbxd.io/fb853e3332f2645fac9d71dc63e09ec1/demo-unbxd700181503576558/search?&q=*&rows={productsPerApiCall}&start={offset}")
        data = r.json()['response']['products']
        for i in data:
            for j in i.keys():
                #all non string and non array values are converted into strings to aid ','.join(iterator) further down the line
                if(type(i[j]) != type([]) and type(i[j]) != type('')):
                    i[j] = str(i[j])
                #bool and not an array and converts it to YES and NO accordingly
                if(type(i[j]) == type('') and i[j].lower() == 'true'):
                    i[j] = 'YES'
                if(type(i[j]) == type('') and i[j].lower() == 'false'):
                    i[j] = 'NO'
                #checks for the condition - array    
                if type(i[j]) == type([]):
                    #all array items converted to str 
                    if(type(i[j][0]) != type('')):
                        for k in range(len(i[j])):
                            i[j][k] = str(i[j][k])
                    #mitigates data duplication
                    aux = {}
                    for l in range(len(i[j])):
                        if(i[j][l] in aux.keys()):
                            aux[i[j][l]] += 1
                        else:
                            aux[i[j][l]] = 1
                    aux2 = []
                    flag = 0
                    if(j == 'unbxd_color_for_category' or j=='colorSwatch' or j=='test_colors' or j=='unbxd_color_mapping' or j=='unbxd_color_for_category') :
                        flag = 1
                        for n in aux.keys():
                            aux2.append(n.split('::')[0])
                    else:
                        #array values w bool values to be converted to YES or NO accordingly
                        for m in aux.keys():
                            if m.lower() == 'true':
                                aux2.append('YES')
                                flag = 1
                            elif m.lower() == 'false':
                                aux2.append("NO")
                                flag = 1
                    if flag==1 :
                        temp = ','.join(aux2)
                        i[j] = temp
                    elif flag==0:
                        temp = ','.join(aux.keys())
                        i[j] = temp
        jsonToLocalCsv(data)
        productCount+=1
        if(productCount >= totalProducts):
            break
        
if __name__ == "__main__":        
    apiReshape()
    uploadToS3('unbxd-2021-interns-test-harsha.csv', 'unbxd-2021-interns-test-harsha')
    print('fin')
