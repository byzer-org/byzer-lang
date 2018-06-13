select array("a","b","c","d","e") as t
as tod_boss_dashboard_sheet_1;

select explode(t) as newt from tod_boss_dashboard_sheet_1
as tod_boss_dashboard_sheet_2;

save overwrite tod_boss_dashboard_sheet_2
as json.`/tmp/abc`
options fileNum="3";