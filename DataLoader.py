# import packages
import pandas as pd
import gzip
import json
import numpy as np
import os
# just load the data , # https://nijianmo.github.io/amazon/index.html

def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)

def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1    
    return pd.DataFrame.from_dict(df, orient='index')

# merge the data according to unqiue_identifer "asin"
def merge_data(review_data, meta_data):
    review_meta_data = pd.merge(
        review_data, meta_data,
        left_on ="asin", right_on = "asin", how = "inner",
        suffixes=('_review', '_meta'))
    print('Number of rows of data: {}'.format(review_meta_data.shape[0]))
    print('Number of cols of data: {}\n'.format(review_meta_data.shape[1]))
    return review_meta_data


def get_data_subset(review_meta_data, desired_features):
    main_data = pd.DataFrame()
    for feature in desired_features:
        ##### Different products may have different kinds of data being collected #####
        # if feature is in the full data
        if feature in review_meta_data.columns:
            main_data[feature] = review_meta_data[feature]
        else:
            # if the feature not found in the full_data, just add np.nan
            main_data[feature] = np.nan
            print('Feature "{}" not found -> values set to np.NaN'.format(feature))
    return main_data

def explore_null(data):
    actual_null = pd.DataFrame((data.isnull().sum()) ).rename(columns ={0:'Actual_Count'})
    normalized_null = pd.DataFrame((data.isnull().sum() / data.shape[0])).rename(columns ={0:'Percentage'})
    null = pd.concat([actual_null, normalized_null], axis=1)
    return null


def load_data(review_data_path, meta_data_path, desired_features =[] , product_name = "", logging=True):
    # get meta_data and review_data separately
    meta_data = getDF(meta_data_path)
    review_data = getDF(review_data_path)
    # merge both data
    review_meta_data = merge_data(review_data, meta_data)
    # Get a subset data from features that we want
    data = get_data_subset(review_meta_data, desired_features)
    if logging:
        print("\nSaving {} data to folder called 'Data' ...".format(product_name))
    outdir = './data'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
        if logging:
            print("Creating a folder called 'Data' ...\n".format(product_name))
    
       # the path that we want to send our data
        outpath = outdir + "/" + product_name + "_data" 
        data.to_csv(outpath)
        if logging:
            print("Csv saved to {}".format(outpath))
    # if file exist
    else:
        print('Data already in {}'.format(outdir))
    return data

