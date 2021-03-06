CREATE  TABLE `deals`(
  `id` int, 
  `deal_created_at` string, 
  `short_title` string, 
  `offer_starts_at` string, 
  `offer_ends_at` string, 
  `aasm_state` string, 
  `type` string, 
  `original_deal_type` string, 
  `deal_types` int, 
  `merchant_id` int, 
  `owned_by` int, 
  `deleted_at` string, 
  `merchant_name` string, 
  `city_id` int, 
  `country_id` int, 
  `expires_on` string, 
  `merchant_primary_zip` int, 
  `category` string, 
  `revenue_at_offer_end` double, 
  `net_revenue_at_offer_end` double, 
  `email_gross_revenue` double, 
  `email_net_revenue` double, 
  `share_gross_revenue_off_ref` double, 
  `share_net_revenue_off_ref` double, 
  `gross_refunds_pre_merchant_payout` double, 
  `net_refunds_pre_merchant_payout` double, 
  `gross_refunds_post_merchant_payout` double, 
  `purchasers` int, 
  `coupons` int, 
  `redeemed_coupons` int, 
  `gift_coupons` int, 
  `auth_fails` int, 
  `mail_sends_empirical_count` int, 
  `mail_sends_position_weighted_count` int, 
  `feature_sends_count` int, 
  `nonfeature_sends_count` int, 
  `mail_opens_count` int, 
  `mail_clicks_count` int, 
  `unsubscriptions` int, 
  `new_purchasers` int, 
  `new_users` int, 
  `email_shares` int, 
  `concept_id` int, 
  `max_category_id` int, 
  `min_category_id` int, 
  `avg_price` double, 
  `min_option_price` double, 
  `max_option_price` double, 
  `coupon_capacity` int, 
  `lat_long_array` array<string>, 
  `is_national_deal` boolean, 
  `destination` int, 
  `limelight` int, 
  `visa` int, 
  `cheerios` int, 
  `national` int, 
  `marked_as_live` int, 
  `side_deal` int, 
  `shipping_required` int, 
  `disable_purchase_feedback` int, 
  `can_run_on_amazon` int, 
  `rerun` int, 
  `dated_event` int, 
  `is_exclusive` int, 
  `shipping_phone_required` int, 
  `no_expiration` int, 
  `charge_immediately_flag` int, 
  `concierge_available` int, 
  `takeout_and_delivery` int, 
  `best_of` int, 
  `unused3` int, 
  `avs` int, 
  `cvv` int, 
  `highly_giftable` int, 
  `regional` int, 
  `activate_immediately` int, 
  `incentivized_deal` int, 
  `live_event` int, 
  `age_restricted` int, 
  `adventure_requires_airline_information` int, 
  `adventure_requires_international_travel` int, 
  `extended_offer` int, 
  `original_offer_ends_at` string, 
  `num_days_to_extend` int, 
  `num_cities` int, 
  `living_social_percentage` double, 
  `salesforce_concept_id` string, 
  `parent_category_id` int, 
  `sub_category_id` int, 
  `run_type` string, 
  `rerun_amazon` int, 
  `fast_track` int, 
  `is_918_deal` int, 
  `amazon_approved` int, 
  `subsidized` int, 
  `renewal` int, 
  `exclude_from_feed` int, 
  `is_adventures_deal` int, 
  `family_friendly` int, 
  `is_gourmet_deal` int, 
  `is_pep_deal` int, 
  `buy_with_confidence` int, 
  `marketplace` int, 
  `image_url` string, 
  `voucher_grant_id` int, 
  `salesforce_opportunity_id` string, 
  `salesforce_account_id` string, 
  `cloned_from` int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
