Extractor is configured to extract from  below: 
1. Locations
2. Reviews
3. Responses

Every extraction run will produce a state file which contains the metadata of the last run. Due to the nature of the large data set, extractor will be running at a limited parameter and it will continue on where it left on from the last run with the parameters stored in the state file.
With the option of `Clear State` enabled, the user will be able to reset the state file and start the extraction from scratch in cases of backfilling. Otherwise, please have this configuration as "false".