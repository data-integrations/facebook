# Facebook ADS batch source

Description
-----------
This plugin used to query Facebook Insights API.

Properties
----------
### General

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Access token:** Access token to be used to authenticate in Facebook API.
### Object query parameters

**Object Id:** Object identifier in Facebook API.

**Object Type:** Object type that represented by **Object Id**.

**Level:** Query level

**Fields:** Fields to be queried.

This fields will be used to generate output record schema.
Note, some of fields will be included in response only if specific breakdown is selected(*age* as example).
See Facebook Insights Api documentation for more information.

**Breakdowns:** Breakdowns to be applied to query.

Note, breakdowns will introduce additional fields to output, user must manually include them to **Fields** property.
For exact mapping between breakdowns and output fields see Facebook Insights Api documentation.

**Sorting:** Sorting rules to be applied to query.
### API limits

**Filtering:** Filers that will specify rules to select objects.

**Time ranges:** Time ranges to be applied to query.
