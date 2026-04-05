----sql----
select 
DISTINCT
fp.*
from facebook_posts fp 
INNER JOIN facebook_reactions fr
ON fp.post_id=fr.post_id
AND fr.reaction='heart'

