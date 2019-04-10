# Introduction
ReviewTrackers is an award-winning customer feedback software company that provides data and technology for tens of thousands of businesses looking to monitor their reviews, manage their business reputation, and drive insights into their customers feedback. Collecting reviews from over 100 sites, ReviewTrackers is an industry leader in helping companies transform the customer experience and convert every customer into a lifelong advocate. This extractor will import reviews from your ReviewTrackers account. Don’t have one? Start a trial [here](https://www.reviewtrackers.com/request-demo/?utm_source=keboola&utm_medium=affiliate&utm_campaign=trial_link). 

# Configuration
The extractor will create three tables in the destination bucket:

    1. Locations
    2. Reviews
    3. Responses

It will bring all the reviews accessible by the user whose credentials are used in the configuration. 

Every extraction run will produce a state file which contains the metadata of the last run. Due to the nature of the large data set, extractor will be running at a limited parameter and it will continue on where it left on from the last run with the parameters stored in the state file. With the option of `Clear State` enabled, the user will be able to reset the state file and start the extraction from scratch in cases of backfilling. Otherwise, please have this configuration as "false".