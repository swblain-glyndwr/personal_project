# Introduction 
NEXT Ads is an initiative to serve personalised adverts to customers browsing the NEXT website.  The primary purpose of the **next-ads** code is to assign each customer the 'best' ad for them. 'Best' can be thought of as the most relevant to them, and that which generates the most incremental revenue for the business.  

# Process
## Macro Process

#### Pre-requisites
Current model scores for all relevant customers and models. These are currently captured within the view [marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_model_scores_latest?o=6188831950334199)

Within the scope of the **next-ads** code:
1. Read the NEXT Ads control sheet, managed by the OSA (On-Site Advertising) team
2. Assign customers to measurement cells
    - Overall: **Fallow Control**
    - Page-wise: **Control**, **Random**, **Personalised**
3. Assign each customer their 'best' division, i.e. that which they have the highest propensity score for
4. Calculate a score for each customer based on the targeting criteria assigned to each active ad
5. For each page, assign an ad for each customer, that is either a **Random** ad, or their **Personalised** ad (depending on the measurement cell), and output the corresonding code for the respective element of the MASID (e.g. `HN1_AABB`)


# Terminology
- The **Random** cell, or a **Random** ad does not indcate a completely randomly assigned ad; these ads are assigned randomly *within* a customer's preferred division.
- 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)