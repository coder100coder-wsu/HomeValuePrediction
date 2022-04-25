import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from pandas import to_numeric
#%matplotlib inline 
import matplotlib 
matplotlib.rcParams["figure.figsize"] = (20,10)
# df1 = pd.read_csv(r"C:\University\DSAD\Project\housepriceprediction\US_Washington_House_Prices.csv")
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['housing']
col = mydb['houseprices']
df1 = pd.DataFrame(list(col.find()))
df1.shape
df1.columns
df2 = df1.drop(['_id','zipcode', 'floors'],axis='columns')
df2.shape
df3= df2.copy()
df3['price_per_sqft'] = to_numeric(df3['price'])/to_numeric(df3['total_sqft'])
df3.head()
df3_stats = df3['price_per_sqft'].describe()
df3_stats
df3.to_csv("whps.csv",index=False)
df3.location = df3.location.apply(lambda x: x.strip())
location_stats = df3['location'].value_counts(ascending=False)
location_stats.values.sum()
len(location_stats[location_stats>100])
location_stats_less_than_100 = location_stats[location_stats<=100]
location_stats_less_than_100
len(df3.location.unique())
df3.location = df3.location.apply(lambda x: 'other' if x in location_stats_less_than_100 else x)
len(df3.location.unique())
df3.head(10)
df3[to_numeric(df3['total_sqft'])/to_numeric(df3['bhk'])<300].head()
df3.shape 
df4 = df3[~(to_numeric(df3['total_sqft'])/to_numeric(df3['bhk'])<300)]
df4.shape
df4.price_per_sqft.describe()
def remove_pps_outliers(df):
    df_out = pd.DataFrame()
    for key, subdf in df.groupby('location'):
        m = np.mean(subdf.price_per_sqft)
        st = np.std(subdf.price_per_sqft)
        reduced_df = subdf[(subdf.price_per_sqft>(m-st)) & (subdf.price_per_sqft<=(m+st))]
        df_out = pd.concat([df_out,reduced_df],ignore_index=True)
    return df_out
df5 = remove_pps_outliers(df4)
df5.shape

def plot_scatter_chart(df,location):
    bdr2 = df[(df.location==location) & (df.bhk==2)]
    bdr3 = df[(df.location==location) & (df.bhk==3)]
    matplotlib.rcParams['figure.figsize'] = (15,10)
    plt.scatter(bdr2.total_sqft,bdr2.price,color='blue',label='2 BDR', s=50)
    plt.scatter(bdr3.total_sqft,bdr3.price,marker='+', color='green',label='3 BDR', s=50)
    plt.xlabel("Total Square Feet Area")
    plt.ylabel("Price ($)")
    plt.title(location)
    plt.legend()
    
plot_scatter_chart(df5,"Kenmore")

df5.bath.unique()
df5[to_numeric(df5['bath'])>to_numeric(df5['bhk'])+2]
df6 = df5[to_numeric(df5['bath'])<to_numeric(df5['bhk'])+2]

df6.shape
df7 = df6.drop(['price_per_sqft'], axis='columns')
df7.head(5)
dummies = pd.get_dummies(df7.location)
dummies.head(3)
df8 = pd.concat([df7,dummies],axis='columns')
df8.head(3)
df9 = df8.drop('location',axis='columns')
df9.head(3)
df9.shape
X= df9.drop(['price'],axis='columns')
X.head(3)
X.shape
y=df9.price
y.head(3)
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2,random_state=10)
from sklearn.linear_model import LinearRegression
lr_clf = LinearRegression()
lr_clf.fit(X_train,y_train)
lr_clf.score(X_test,y_test)
from sklearn.model_selection import ShuffleSplit
from sklearn.model_selection import cross_val_score
cv = ShuffleSplit(n_splits=5, test_size=0.2, random_state=0)
cross_val_score(LinearRegression(), X, y, cv=cv)

def predict_price(location,total_sqft,bath,bhk):    
    loc_index = np.where(X.columns==location)[0][0]

    x = np.zeros(len(X.columns))
    x[0] = total_sqft
    x[1] = bath
    x[2] = bhk
 #   x[3] = floors
    if loc_index >= 0:
        x[loc_index] = 1

    return lr_clf.predict([x])[0]

predict_price('Kenmore',1200, 2, 2)

import pickle
with open('us_washington_house_prices_model.pickle', 'wb') as f:
    pickle.dump(lr_clf,f)

import json
columns = {
    'data_columns' : [col.lower() for col in X.columns]
}
with open("uscolumns.json","w") as f:
    f.write(json.dumps(columns))




