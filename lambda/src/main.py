# Import pandas and numpy library 
import pandas as pd 
import numpy as np
import json

def handle(event,context):
    try:    
        print("Received event: " + json.dumps(event, indent=2))
        
        request = json.dumps(event, indent=2)
        #print(request)
        df = pd.DataFrame([pd.read_json(request, typ='series')])	
        #print(df)
        ## create an array
        array1= np.array([df.CaseAction1,df.CaseAction2,df.CaseAction3,df.CaseAction4,df.CaseAction5])
        array2= np.array([df.Interestrate1,df.Interestrate2,df.Interestrate3,df.Interestrate4,df.Interestrate5])
        array3= np.array([df.Coreprodukt1,df.Coreprodukt2,df.Coreprodukt3,df.Coreprodukt4,df.Coreprodukt5])
        
        #Calucating the number of New products
        count=0
        for i in range(len(array1)):
            #print(array1[i])
            if array1[i]== 'NY': 
                count+=1   
    
        #print(count)
        df['CountNY']=count
        #Calucating the existing interest rate
        intrest=20
        for i in range(len(array1)):
            #print(array1[i])
            if array1[i]== 'INNFRIELSE': 
                df['ExistingIntrst']= min(intrest,array2[i])
        #print(df['ExistingIntrst'])
        #Calucating the LTVADJINNFRIELSE
        LTVADJINNFRIELSE=0
        for i in range(len(array1)):
            #print(array1[i])
            if array1[i]== 'INFREIELSE' and array3[i]  in ('PUTHOVED','PUTTOPP'): 
                LTVADJINNFRIELSE=1
                break
        df['LTVADJINNFRIELSE']=LTVADJINNFRIELSE
        #print(LTVADJINNFRIELSE)
        #ListPriceIND
        df['LISTPRICEPTIND1']=0
        df['LISTPRICEPTIND2']=0
        df['LISTPRICEPTIND3']=0
        df['LISTPRICEPTIND4']=0
        df['LISTPRICEPTIND5']=0
        array4= np.array([df.LISTPRICEPTIND1,df.LISTPRICEPTIND2,df.LISTPRICEPTIND3,df.LISTPRICEPTIND4,df.LISTPRICEPTIND5])
        #Calucating the number of New products
        count=0
        for i in range(len(array1)):
            #print(array1[i])
            if array1[i]== 'NY': 
                count+=1 
        #print(count)
        df['CountNY']=count
        #Calucating the existing interest rate
        intrest=20
        for i in range(len(array1)):
            #print(array1[i])
            if array1[i]== 'INNFRIELSE': 
                df['ExistingIntrst']= min(intrest,array2[i])
        #print(df['ExistingIntrst'])
        for i in range(len(array1)):
            #print(array1[i])
            if  array3[i] in  ('PFUBS960', 'PFUBS075', 'PFUBS375', 'PFUBS575', 
                              'PFUFS075', 'PFUFS375', 'PFUFS375', 'PFUFS575', 'PFUFS575', 'PFUSA075', 
                           'PFUSA375', 'PFUSA575', 'PUTMANSA', 'PUTMF100', 'PUTBLANC', 'PUTBUBAS', 
                             'PUTESTUD', 'PUTGARKO', 'PUTKAUSE', 'PUTANBIL')  : 
                array4[i] =1
                #print(array4[i])
            else :
                array4[i] =0
                #print(array4[i])
                    #Calucating the INDCTBPNY
            INDCTBPNY=0
            for i in range(len(array1)):
                #print(array1[i])
                if array1[i]== 'NY' and array3[i]  in ('PUTHOVED','PUTTOPP'): 
                    INDCTBPNY=1
                    break
            df['INDCTBPNY']=1   
            #print(INDCTBPNY)
            for index, row in df.iterrows():
                if df.loc[index, 'LTVADJINNFRIELSE']  == 1 and df.loc[index, 'INDCTBPNY'] == 0 :
                    df['Delta']=1
                else :
                    df['Delta']=0
            
            df['NPrice'] = df.loc[:,['ExistingIntrst','Delta']].sum(axis=1)
            
            array6=np.array([df.NPrice])
            array8= np.array(['OFFEREDPRICE1','OFFEREDPRICE2','OFFEREDPRICE3','OFFEREDPRICE4','OFFEREDPRICE5'])
            #Calucating the offered Price
            for i in range(len(array1)):
                #print(array1[i])
                if array1[i]== 'NY' : 
                    if array4[i]==1:
                        df[array8[i]]=array2[i]
                        print(array8[i],array2[i])
                    else:
                        df[array8[i]]=min(array6[0],array2[i])
                        print(array6[0],array2[i])
                        print(array8[i])   
        response = json.loads(json.dumps(df.to_json(orient='records')))
        print(response)
        return {
            'statusCode': 200,
            'isBase64Encoded':False,
            'body': json.dumps(response)
        }
    except Exception as err:
        return {
            'statusCode': 400,
            'isBase64Encoded':False,
            'body': 'Call Failed {0}'.format(err)
        }
