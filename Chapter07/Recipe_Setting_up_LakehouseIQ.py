
# Recipe: Setting up LakehouseIQ

#• Requesting sales data
#You can use LakehouseIQ to receive sales data for a particular query as in this example:
# Pseudo-code for illustration response = lakehouseiq.
ask("What is the sales comparison between our summer and fall collections?") print(response.summary)


#You may want to use LakehouseIQ to adjust your inventory and receive recommendations, as
#in the following example:
# Pseudo-code for illustration inventory_advice = lakehouseiq.
suggest_inventory_adjustments("spring collection")
print(inventory_advice.recommendations)