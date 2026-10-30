create materialized view if not exists riot.game.gold_champion as
select version, count(*) as champion_count
from riot.game.silver_champions
group by version;