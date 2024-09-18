
library(dplyr);



### Načtení sněmovních a senátních open dat.

stisk <- read.table('../data/src/stisky/se_tisk.unl', sep = '|',
                    fileEncoding = 'Windows-1250');
stisk <- stisk[, -8];
colnames(stisk) <- c('id_tisk', 'id_druh', 'id_stav', 'cislo_tisku', 'id_obdobi', 
                     'anotace', 'nazev_tisku'); 
pstisk <- read.table('../data/src/pstisky/tisky_new.unl', sep = '|',
                     fileEncoding = 'Windows-1250');
pstisk <- pstisk[, -25];
colnames(pstisk) <- c('id_tisk', 'id_druh', 'id_stav', 'ct', 'cislo_za', 'id_navrh',
                  'id_org', 'id_org_obd', 'id_osoba', 'navrhovatel', 'nazev_tisku',
                  'predlozeno', 'rozeslano', 'dal', 'tech_nos_dat', 'uplny_nazev_tisku',
                  'zm_lhuty', 'lhuta', 'rj', 't_url', 'is_eu', 'roz', 'is_sdv',
                  'status');

pstisk2stisk <- read.table('../data/src/stisky/psp2senat.unl', sep = '|',
                    fileEncoding = 'UTF-8');
pstisk2stisk <- pstisk2stisk[, -4];
colnames(pstisk2stisk) <- c('id_psp', 'id_senat', 'typ');

hist <- read.table('../data/src/pstisky/hist.unl', sep = '|',
                           fileEncoding = 'Windows-1250');
hist <- hist[, -16];
colnames(hist) <- c('id_hist', 'id_tisk', 'datum', 'id_hlas', 'id_prechod',
                    'id_bod', 'schuze', 'usnes_ps', 'orgv_id_posl', 'ps_id_posl',
                    'orgv_p_usn', 'zaver_publik', 'zaver_sb_castka',
                    'zaver_sb_cislo', 'poznamka');

prechody <- read.table('../data/src/pstisky/prechody.unl', sep = '|',
                   fileEncoding = 'Windows-1250');
prechody <- prechody[, -6];
colnames(prechody) <- c('id_prechod', 'odkud', 'kam', 'id_akce', 'typ_prechodu');

stavy <- read.table('../data/src/pstisky/stavy.unl', sep = '|',
                       fileEncoding = 'Windows-1250');
stavy <- stavy[, -7];
colnames(stavy) <- c('id_stav', 'id_typ', 'id_druh', 'popis', 'lhuta', 'lhuta_where');

typstavu <- read.table('../data/src/pstisky/typ_stavu.unl', sep = '|',
                    fileEncoding = 'Windows-1250');
typstavu <- typstavu[, -3];
colnames(typstavu) <- c('id_typ', 'popis_stavu');

typakce <- read.table('../data/src/pstisky/typ_akce.unl', sep = '|',
                       fileEncoding = 'Windows-1250');
typakce <- typakce[, -3];
colnames(typakce) <- c('id_akce', 'popis_akce');

druhtisku <- read.table('../data/src/pstisky/druh_tisku.unl', sep = '|',
                      fileEncoding = 'Windows-1250');
druhtisku <- druhtisku[, -4];
colnames(druhtisku) <- c('id_druh', 'druh_t', 'nazev_druh');
druhtisku$druh_t <- gsub('T', 'hlavní', druhtisku$druh_t);
druhtisku$druh_t <- gsub('Z', 'následný', druhtisku$druh_t);
druhtisku$druh_t <- gsub('X', 'historický', druhtisku$druh_t);



### Spojení tabulek s číselníky.


## Spojení historie tisků s číselníky.

# Spojení historie s přechody. 
df1 <- left_join(hist, prechody, by = 'id_prechod');

# Spojení přechodů se stavy.
colnames(stavy)[1] <- 'odkud';
df2 <- left_join(df1, stavy, by = 'odkud');

colnames(stavy)[1] <- 'kam';
df3 <- left_join(df2, stavy, by = 'kam');

colnames(stavy)[1] <- 'id_stav';

# Spojení stavů s názvy stavů.
colnames(typstavu)[1] <- 'id_typ.x';
df4 <- left_join(df3, typstavu, by = 'id_typ.x');

colnames(typstavu)[1] <- 'id_typ.y';
df5 <- left_join(df4, typstavu, by = 'id_typ.y');

colnames(typstavu)[1] <- 'id_typ';

# Spojení přechodů s názvem akce.
df6 <- left_join(df5, typakce, by = 'id_akce');

# Historie tisků s hodnotami, osekání na relevantní sloupce
df7 <- data.frame(df6$id_hist, df6$id_tisk, df6$datum, df6$popis_stavu.x,
                 df6$popis_stavu.y, df6$popis_akce);  
colnames(df7) <- c('id_hist', 'id_tisk', 'datum', 'odkud', 'kam', 'akce');


## Spojení historie tisků se sněmovními tisky a číselníkem druhů tisků

# Spojení historie tisků se sněmovními tisky.
df8 <- left_join(df7, pstisk, by = 'id_tisk');

# Výběr relevantních sloupců.
df9 <- data.frame(df8$id_tisk, df8$id_hist, df8$id_org_obd, df8$nazev_tisku, df8$uplny_nazev_tisku,
                  df8$id_druh, df8$ct, df8$cislo_za, df8$navrhovatel,
                  df8$datum, df8$odkud, df8$kam, df8$akce);
colnames(df9) <- c('id_tisk', 'id_hist', 'ps_obdobi', 'nazev', 'uplny_nazev', 'id_druh',
                   'cislo', 'cislo_za', 'navrhovatel', 'datum', 'odkud', 'kam',
                   'akce');

# Spojení historie tisků a sněmovních tisků s číselníkem druhů tisků.
df10 <- left_join(df9, druhtisku, by = 'id_druh');

df11 <- data.frame(df10$id_tisk, df10$id_hist, df10$ps_obdobi, df10$nazev, df10$uplny_nazev,
                   df10$nazev_druh, df10$cislo, df10$cislo_za, df10$navrhovatel,
                   df10$datum, df10$odkud, df10$kam, df10$akce);
colnames(df11) <- c('id_tisk', 'id_hist', 'ps_obdobi', 'nazev', 'uplny_nazev', 'druh',
                   'cislo', 'cislo_za', 'navrhovatel', 'datum', 'odkud', 'kam',
                   'akce');


## Připojení senátních tisků (kvůli období)

# Spojení historie tisků a sněmovních tisků s převodníkem na senátní tisky
colnames(pstisk2stisk)[1] <- 'id_tisk';
df12 <- left_join(df11, pstisk2stisk, by = 'id_tisk');

# Příprava senátních tisků a spojení s historií tisků.
stiskobd <- data.frame(stisk$id_tisk, stisk$id_obdobi); 
colnames(stiskobd) <- c('id_senat', 's_obdobi');
df13 <- left_join(df12, stiskobd, by = 'id_senat');
df13 <- data.frame(df13$id_tisk, df13$id_senat, df13$id_hist, df13$ps_obdobi, 
                   df13$s_obdobi, df13$nazev, df13$uplny_nazev, df13$druh, 
                   df13$cislo, df13$cislo_za, df13$navrhovatel, df13$datum, 
                   df13$odkud, df13$kam, df13$akce);
colnames(df13) <- c('id_ps', 'id_s', 'id_hist', 'ps_obdobi', 's_obdobi', 'nazev', 
                    'uplny_nazev', 'druh', 'cislo', 'cislo_za', 'navrhovatel', 
                    'datum', 'odkud', 'kam', 'akce');

# Přepis sněmovních i senátních období na roky.
df13$ps_obdobi <- gsub('165', '1992-1996', df13$ps_obdobi);
df13$ps_obdobi <- gsub('166', '1996-1998', df13$ps_obdobi);
df13$ps_obdobi <- gsub('167', '1998-2002', df13$ps_obdobi);
df13$ps_obdobi <- gsub('168', '2002-2006', df13$ps_obdobi);
df13$ps_obdobi <- gsub('169', '2006-2010', df13$ps_obdobi);
df13$ps_obdobi <- gsub('170', '2010-2013', df13$ps_obdobi);
df13$ps_obdobi <- gsub('171', '2013-2017', df13$ps_obdobi);
df13$ps_obdobi <- gsub('172', '2017-2021', df13$ps_obdobi);
df13$ps_obdobi <- gsub('173', '2021+', df13$ps_obdobi);

df13$s_obdobi <- gsub('^1$', '1996-1998', df13$s_obdobi);
df13$s_obdobi <- gsub('^2$', '1998-2000', df13$s_obdobi);
df13$s_obdobi <- gsub('^3$', '2000-2002', df13$s_obdobi);
df13$s_obdobi <- gsub('^4$', '2002-2004', df13$s_obdobi);
df13$s_obdobi <- gsub('^5$', '2004-2006', df13$s_obdobi);
df13$s_obdobi <- gsub('^6$', '2006-2008', df13$s_obdobi);
df13$s_obdobi <- gsub('^7$', '2008-2010', df13$s_obdobi);
df13$s_obdobi <- gsub('^8$', '2010-2012', df13$s_obdobi);
df13$s_obdobi <- gsub('^9$', '2012-2014', df13$s_obdobi);
df13$s_obdobi <- gsub('^10$', '2014-2016', df13$s_obdobi);
df13$s_obdobi <- gsub('^11$', '2016-2018', df13$s_obdobi);
df13$s_obdobi <- gsub('^12$', '2018-2020', df13$s_obdobi);
df13$s_obdobi <- gsub('^13$', '2020-2022', df13$s_obdobi);
df13$s_obdobi <- gsub('^14$', '2022-2024', df13$s_obdobi);


## Doplnění instituce, která zákon navrhla.

# Nejlépe odečíst z úplného názvu zákona. 
df13$navrh_inst <- gsub('^(V|v)l(a)*ádní.*', 'Vláda', df13$uplny_nazev);
df13$navrh_inst <- gsub('^(Senátní|Návrh Senátu).*', 'Senát', df13$navrh_inst);
df13$navrh_inst <- gsub('^Návrh posl(\\.|ance|anců|ankyně|ankyň).*', 'Poslanecká sněmovna', df13$navrh_inst);
df13$navrh_inst <- gsub('^Návrh (Z|z)astupitelstva.*', 'Kraje', df13$navrh_inst);

# Zařadit nový sloupec na místo hned za navrhovatelem.
df13 <- df13[, c(1:11, 16, 12:15),];


## Final dataset pro analýzu
df <- df13;

rm(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13,
   druhtisku, hist, prechody, pstisk, pstisk2stisk, stavy, stisk, typakce, 
   typstavu, stiskobd);

write.csv(df, '../data/out/tisky.csv' , row.names = F, fileEncoding = 'UTF-8');
