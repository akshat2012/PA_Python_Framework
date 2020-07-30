query_dict = {
    'payout_level_info': '''
USE realtime_hudi_api;
SELECT pa.id,
	cast(FROM_UNIXTIME(pa.created_at +19800) as timestamp) as created_at,
	cast(FROM_UNIXTIME(pa.processed_at +19800) as timestamp) as processed_at,
	cast(FROM_UNIXTIME(pa.initiated_at +19800) as timestamp) as initiated_at,
	cast(FROM_UNIXTIME(pa.reversed_at +19800) as timestamp) as reversed_at,
	cast(FROM_UNIXTIME(COALESCE(pa.processed_at,pa.reversed_at) +19800) as timestamp) as finished_at,
	pa.status, pa.mode, pa.channel,
	erc.error_bucket

FROM realtime_hudi_api.payouts as pa
JOIN realtime_hudi_api.balance as ba
	ON (pa.balance_id = ba.id AND ba.type = 'banking')
JOIN realtime_hudi_api.merchants me
	ON (pa.merchant_id = me.id AND me.business_banking = 1)
JOIN aggregate_pa.x_merchant_fact as xmf
	ON me.id = xmf.merchant_id
LEFT JOIN realtime_fts_live.transfers as fts_tr
	ON (pa.id = fts_tr.source_id and fts_tr.source_type = 'payout')
LEFT JOIN aggregate_pa.payouts_error_code_mapping  AS erc
	ON (erc.gateway = (CASE
						  WHEN pa.channel = 'icici' and pa.mode = 'IMPS' 
							  THEN 'icici_imps'
						  WHEN pa.channel = 'yesbank' and pa.mode = 'UPI' 
							  THEN 'yesbank_upi'
						  ELSE 
							  pa.channel 
						  END)
		AND COALESCE(fts_tr.gateway_error_code, 'Default') = erc.gateway_error_code)
where cast(FROM_UNIXTIME(COALESCE(pa.processed_at,pa.reversed_at) +19800) as timestamp) between 
            cast(%s as timestamp) and cast(%s as timestamp)
and ((CASE
		WHEN pa.status = 'processed'
			THEN NULL
		ELSE 
			COALESCE(erc.error_bucket, 'Unknown')
	END NOT IN ('Merchant', 'NPCI', 'Beneficiary bank'))
	OR 
	(CASE
		WHEN pa.status = 'processed'
			THEN NULL
	 	ELSE 
			COALESCE(erc.error_bucket, 'Unknown')
	END) IS NULL);'''
}


def get_query(key):
    return query_dict[key]