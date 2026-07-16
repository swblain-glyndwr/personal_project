

# def realtime_reranking_model(input_rpid: str, input_features: dict)-> dict: 
    #   TODO: Decide if we need to replicate any 'post-processing steps!?' 



def realtime_reranking_advert_data_prep(input_rpid):

    # For TESTING 
    input_rpid='4209253304'
    PageTypeFilters= ["ProductListingPage"]

    from pyspark.sql import functions as F

    #default_variables 
    account_number=None
    target=False
    #Tables needed 

    tbls={"rpid_account": "marketingdata_prod.warehouse.rpid_with_accounts",
        "preranked_ads": "marketingdata_prod.warehouse.next_uk_nextads_preranked_ads_from_themes_v2_latest", 
        "customer_cells": "marketingdata_prod.warehouse.next_uk_nextads_customer_cells_latest",
        "product_table": "marketingdata_prod.warehouse.product_catalog"}
    
    #1 find customer 
    #TODO this is sooooo slow (10s)
    rpid_table=spark.table(tbls["rpid_account"])

    account_number=(rpid_table.filter(F.col("roamingprofileid")==input_rpid)).select(F.col("account_number")).collect()
    if not account_number: 
        logger.info("No account found")
        return 
    account_number=account_number[0][0]


    #2 check if they are control or targeted 
    #TODO: Add additional flags/checks here for which ads we want to filter to! 
    cells_table=spark.table(tbls["customer_cells"])

    customer_details=(cells_table.filter(F.col("AccountNumber")==account_number).select(F.col("FallowControl")
                                                                                    ,F.col("PageTypeIsolation")
                                                                                    ,F.col("HomePageTest1")
                                                                                    ,F.col("ShoppingBagTest1")
                                                                                    ,F.col("OrderCompleteTest1")
                                                                                    , F.col("LandingPageTest1"))
                                                                                    
                                                    .collect())
    if customer_details: 
        target=True if customer_details[0][0]=="Ads" else False
    if not target:
        logger.info("Account not in target group")
        return 
    
    #3 Filter batch adverts to current customer 
    # TODO: Need to build a table of features associated with the levels we are interested in to join here! 
    current_ranked_ads=spark.table(tbls["preranked_ads"])
    customer_ads=(current_ranked_ads
                .filter(F.col("AccountNumber")==account_number)
                .filter(F.col("PageType").isin(PageTypeFilters)))

    if customer_ads.isEmpty(): 
        logger.error("No current ads found for location")
    return customer_ads


def realtime_reranking_item_data_prep(input_features: dict): 

    from pyspark.sql import functions as F
    #Testing
    input_features= {1 :{"item":"v12037",
                         "action": "view"},
                    2:{"item": "w87234",
                       "action": "view"},
                    3: {"item": "w03942",
                        "action":"view"}}
    # Variables 
    items =[]
    items_data=None

    tbls={"product_table": "marketingdata_prod.warehouse.product_catalog"}
    product_columns=["pid", "brand", "next_category", "department" ]


    items=[value.get("item").upper() for value in input_features.values()]
    
    if not items: 
        logger.error("No items identified")
        return
    #2 Filter item table to provided features
    
    #TODO: Might want a distinct view of these only at PID level for speed 

    prod_table=spark.table(tbls["product_table"])
    items_data= prod_table.filter(F.col("pid").isin(items)).select(product_columns).distinct()

    #TODO: check if this works 
    if not items_data: 
        logger.error("Items not found in dataset")
        return 
    
    # TODO: need to pivot as well here



    # TODO: build a dataset of weightings 
    #Join to the dataset of weightings to be applied? 
    items_weights=items_data.join("")
    # Return data set for weighting 

    return items_data
     


#3 Cross item table with weighting factors 



def realtime_reranking_item_cross(): 

#4 Cross weighting factors with batch adverts 


#5 Rerank weighting factors 




#6 Update items 


#7 Return items

