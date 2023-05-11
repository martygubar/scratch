# data updates

adwc4pm->San Jose->moviestream compartment

admin/bigdatapm2019#
moviestream/bigdatapm2019#



Churn Storyline
* Base it on views - not sales
* When based on sales previously - we had 11,000 potential churners
* Today, everyone had at least one click in December, November

Churner base profile
Backfiring: Millennials are a top market
Pay subscriptions 
Increasing pay ratio or lowering discounts are causing us to lose those customers.
* Age range: between 28 and 40
* Work experience > 5 years
* Yrs current employer < 4 years
* Yrs current residence < 6 years
* Insufficient funds > 0
* Action Adventure > 25 views
* Paid ratio trend increasing
* Discount ratio decreasing

Customers with this profile: 3778 (2.5%)
Total customers:  148,000

How to update the data set q's
1. Pick 2000 out of the 3778 customers and zero out their views in December
2. Add an attribute that has customers that live near churners?
3. Pick a random sampling of 500 other customers and zero out their views in December

Profile q's: 
Add derived attributes
* Add an attribute for customers that live near churners (boolean)
* Add a boolean attribute on trending (views have been trending down)
* Challenge - each time we add attributes, it can impact the # of customers that fit the profile and requires a lot of data massaging


7500 customers meet the profile above

1. Create November churners - use this to create the predictive model
    1. Need declining views in October. October views will be lower than the average of other months
    2. 50% of that customer profile churns. Views in November are 0
    3. Add a small random sampling of customers. They will live near the 50%
2. Create December churners - we will predict December - score these customers and see how accurate it was
    1. 50% of that profile churns??  These customers showed a reduction as well
    2. Also, 1000 customers that are within 1 mile of the November churners also churned
3. Future
    1. 

Do This:
1. Take all of the churners and a sample of the non-churners and build a model over those guys.
2. There should be a binary column specifying whether or not they are Binary column (0-1) if they are churner.
4. Then apply the model to all non-churners - who are in a separate table

