# Entity Extraction


## Extract

* Fetch N articles
* Process 500 articles entities per minute into `story_entity_df`
* Set character length of 196
* Fetch all `apis_entity` that are not in `storyentityref` and input it into `storyentityref`
* Fetch and add existing entities in `api_entity` to `entity_df`
* Merge `entity_df` and `story_entity_df` into `merged_df` based on extracted entity
* If it doesn't exists in apis_entity table check existence in `apis_storyentityref`, results into `story_entity_ref_df`
* Left join `merged_df` and `story_entity_ref_df` based on extracted entity

## Insert

* Find and input `TYPE_IDS`
* Entities with null values on merged_df are new `storyentityref`
* Generate new UUIDs for `storyentityref`
* Input MAP and REF


# Entity Extraction w/ Fuzzy


## Extract

* Fetch N articles
* Process 500 articles entities per minute into `story_entity_df`
* Set character length of 196
* Fetch all `apis_entity` that are not in `storyentityref` & `entity_alias` and input it into `storyentityref` & `entity_alias`
* Fetch and add existing alias with parentID (from `entity_alias`) in `api_entity` to `entity_df`
* Left join `story_entity_df` with `entity_df`  into `merged_df` based on extracted entity
* If it doesn't exists in apis_entity table check existence in `entity_alias`, results into `story_entity_ref_df` with UUID, name & ParentID
* Left join `merged_df` and `story_entity_ref_df` based on extracted entity name


## Fuzzy

* All the unmatched entities are either new or can be tried to be matched with wiki or alias
* Fetch all alias in the database locally
* Fuzzy match the unmatched entities using RapidFuzz
* If match found store score, parentID
* Add these matched items to the `ALIAS` list so that next time it'd be matched in the EXTRACT stage
* In `merged_df` map the parentID of found entities as `entity_id`


## Insert

* Find and input `TYPE_IDS`
* Entities with null values on merged_df are new `storyentityref` (parent) and corresponding `entity_alias`
* Generate new UUIDs for `storyentityref`
* Input ALIAS, MAP and REF
