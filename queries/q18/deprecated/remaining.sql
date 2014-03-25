-------------------------------------------------------------------------------
--------------------------------REMAINING--------------------------------------
-------------------------------------------------------------------------------
-- identify stores with declining sales---
-- and extract sentiment regarding the stores with declining sales, if any
select s_store_name, pr_review_date,out_content, out_polarity, out_sentiment_words
from ExtractSentiment
(
	on (
		select 
			s_store_name
			,pr_review_content
			,pr_review_date
		from 
			store_coefficient c
			,store100 s 
			,product_reviews100
		where 
			c.slope < 0 
			and s.s_store_sk = c.store
			and pr_review_content like '%'||s_store_name||'%'
	)
	text_column('pr_review_content')
	model('dictionary')
	level('DOCUMENT')
	accumulate('s_store_name','pr_review_date')
)
where out_polarity = 'NEG';

drop view store_coefficient;
drop view time_series_store;

end;
