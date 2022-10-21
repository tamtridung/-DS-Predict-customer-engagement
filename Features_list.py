def generateFeatures(transaction, users):
    feature_list = [
        # HISTORICAL ACTIVITIES
        activity.f_user_transcAmount_min(transc_table=transaction), #
        activity.f_user_transcAmount_max(transc_table=transaction), #
        activity.f_user_transcAmount_total(transc_table=transaction),#
        activity.f_user_transcAmount_avg(transc_table=transaction),#

        # TIME AND RECENCY 
        recency.f_user_daysSinceLastTransc(transc_table=transaction), #
        recency.f_user_transcCurrMonth_count(transc_table=transaction), #
        recency.f_user_transcPrevMonth_count(transc_table=transaction),
        recency.f_transcByDayOfWeek_ratio(transc_table=transaction), #
        recency.f_transcByTimeOfDay_ratio(transc_table=transaction), #
        recency.f_user_merchantTranscLastMonth_count(transc_table=transaction), #X
        recency.f_user_nonMerchantTranscCurrMonth_count( #X
            transc_table=transaction),
        recency.f_user_transcAmountCurrMonth_total(transc_table=transaction), #
        recency.f_user_transcAmountPrevMonth_total(transc_table=transaction), #X
        recency.f_user_transcByDirectionCurrMonth_count( #X
            transc_table=transaction),
        recency.f_user_transcByDirectionPrevMonth_count(#X
            transc_table=transaction),
        recency.f_user_transcByStatusCurrMonth_count(transc_table=transaction),#X
        recency.f_user_transcByStatusPrevMonth_count(transc_table=transaction),#X
        recency.f_user_transcByTypeCurrMonth_count(transc_table=transaction),#X
        recency.f_user_transcByTypePrevMonth_count(transc_table=transaction),#X

        # LOCATION
        location.f_user_localTranscAmount_avg(transc_table=transaction),#X
        location.f_user_intlTranscAmount_avg(transc_table=transaction),#X
        location.f_user_freqIntlTransc_ratio(transc_table=transaction),#X#X
        location.f_user_freqLocalTransc_ratio(transc_table=transaction),#X
        location.f_user_topForeign_country(transc_table=transaction),#X#X
        location.f_user_topMerchant_city(transc_table=transaction),#X
#X
        # PREFERENCES AND DEMOGRAPHIC
        demographic.f_user_ageBucket(transc_table=transaction), #
        demographic.f_user_plan(),#x
        demographic.f_user_daysSinceJoined(transc_table=transaction), #
        demographic.f_user_daysSinceLastTransc_quantiles( 
            transc_table=transaction),
        demographic.f_user_contact_count(), #X
        demographic.f_user_device(), # X
        demographic.f_user_home_city(), # X
        demographic.f_user_home_country() # X
    ]

    _feat_cols = [[i.feature_names] if isinstance(
        i.feature_names, str) else i.feature_names for i in feature_list]
    feature_columns = ["user_id"] + list(itertools.chain(*_feat_cols))

    feature_pipeline = Pipeline(stages=feature_list)
    features = (feature_pipeline.fit(users)
                                .transform(users)
                                .select(feature_columns))

    return features
