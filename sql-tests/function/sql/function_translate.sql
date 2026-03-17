-- Migrated from dev/test/sql/test_function/T/test_translate
-- Test Objective:
-- 1. Validate TRANSLATE() function with ASCII and Unicode (CJK, emoji) character sets.
-- 2. Verify correct behavior when to_str is longer/shorter than from_str.
-- 3. Verify NULL and empty string handling.
-- 4. Verify error for wrong number of arguments.
-- 5. Validate large string handling near OLAP_STRING_MAX_LENGTH boundary.
-- 6. Validate multi-chunk processing with table-based inputs.
-- 7. Validate random data with various constant/non-constant argument combinations.
-- 8. Validate UTF-8 constant from_str and to_str permutations.

------------------------------------------------------------------------
-- Section 1: test_translate_exceed_OLAP_STRING_MAX_LENGTH
------------------------------------------------------------------------

-- query 1
-- Exceed OLAP_STRING_MAX_LENGTH: constant args, same-byte replacement
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(REPEAT('a', 1024*1024), 'a', 'b')
)), 0);

-- query 2
-- Exceed OLAP_STRING_MAX_LENGTH: constant args, multi-byte to single-byte
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(CONCAT(REPEAT('a', 1024*1024-1024*3), REPEAT('膨', 1024)), '膨', 'p')
)), 0);

-- query 3
-- Exceed OLAP_STRING_MAX_LENGTH: constant args, single-byte to multi-byte
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(CONCAT('a', REPEAT('b', 1024*1024 - 1 - 2)), 'a', '膨')
)), 0);

-- query 4
-- Expansion beyond max length: may return NULL
-- @skip_result_check=true
USE ${case_db};
SELECT TRANSLATE(REPEAT('a', 1024*1024), 'a', '膨');

-- query 5
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('placeholder', 'a', 'b');

-- query 7
-- Exceed OLAP_STRING_MAX_LENGTH: table-based, same-byte replacement
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(REPEAT('a', 1024*1024), from_str, to_str)
)), 0) from t1;

-- query 8
-- @skip_result_check=true
USE ${case_db};
truncate table t1;

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('placeholder', '膨', 'p');

-- query 10
-- Exceed OLAP_STRING_MAX_LENGTH: table-based, multi-byte to single-byte
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(CONCAT(REPEAT('a', 1024*1024-1024*3), REPEAT('膨', 1024)), from_str, to_str)
)), 0) from t1;

-- query 11
-- @skip_result_check=true
USE ${case_db};
truncate table t1;

-- query 12
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('placeholder', 'a', '膨');

-- query 13
-- Exceed OLAP_STRING_MAX_LENGTH: table-based, single-byte to multi-byte
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(CONCAT('a', REPEAT('b', 1024*1024 - 1 - 2)), from_str, to_str)
)), 0) from t1;

-- query 14
-- @skip_result_check=true
USE ${case_db};
truncate table t1;

-- query 15
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('placeholder', 'a', '膨');

-- query 16
-- Exceed OLAP_STRING_MAX_LENGTH: table-based, expansion
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(
    TRANSLATE(REPEAT('a', 1024*1024), from_str, to_str)
)), 0) from t1;

------------------------------------------------------------------------
-- Section 2: test_translate_to_str_greater_than_from_str
------------------------------------------------------------------------

-- query 17
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 18
-- to_str longer than from_str
USE ${case_db};
SELECT TRANSLATE('a|b|c', 'ab', '123');

-- query 19
-- from_str longer than to_str (chars removed)
USE ${case_db};
SELECT TRANSLATE('a|b|c', 'abc', '12');

-- query 20
-- ASCII to CJK
USE ${case_db};
SELECT TRANSLATE('C|S', 'CS', '测试');

-- query 21
-- CJK to ASCII
USE ${case_db};
SELECT TRANSLATE('测|试', '测试', 'CS');

-- query 22
-- CJK to shorter ASCII (one char removed)
USE ${case_db};
SELECT TRANSLATE('测|试', '测试', 'C');

-- query 23
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 24
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('a|b|c', 'ab', '123'),
    ('a|b|c', 'abc', '12'),
    ('C|S', 'CS', '测试'),
    ('测|试', '测试', 'CS'),
    ('测|试', '测试', 'C');

-- query 25
-- to_str greater/less than from_str: table-based
USE ${case_db};
SELECT TRANSLATE(src, from_str, to_str) from t1;

------------------------------------------------------------------------
-- Section 3: test_translate_multiple_chunks
------------------------------------------------------------------------

-- query 26
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 27
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 28
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 SELECT CONCAT('a|b|c', generate_series), 'ab', '123' FROM TABLE(generate_series(1, 4095*2));

-- query 29
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 SELECT CONCAT('a|b|c', generate_series), 'abc', '12' FROM TABLE(generate_series(1, 4095*2));

-- query 30
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 SELECT CONCAT('C|S', generate_series), 'CS', '测试' FROM TABLE(generate_series(1, 4095*2));

-- query 31
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 SELECT CONCAT('测|试', generate_series), '测试', 'CS' FROM TABLE(generate_series(1, 4095*2));

-- query 32
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 SELECT CONCAT('测|试', generate_series), '测试', 'C' FROM TABLE(generate_series(1, 4095*2));

-- query 33
-- Multi-chunk: all non-constant args
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, from_str, to_str))), 0) from t1;

-- query 34
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'ab', '123'))), 0) from t1;

-- query 35
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'abc', '12'))), 0) from t1;

-- query 36
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'CS', '测试'))), 0) from t1;

-- query 37
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '测试', 'CS'))), 0) from t1;

-- query 38
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '测试', 'C'))), 0) from t1;

------------------------------------------------------------------------
-- Section 4: test_translate_empty
------------------------------------------------------------------------

-- query 39
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 40
-- Empty to_str removes matched chars
USE ${case_db};
SELECT TRANSLATE('abc', 'ab', '');

-- query 41
-- Empty from_str: no replacement
USE ${case_db};
SELECT TRANSLATE('abc', '', 'ab');

-- query 42
-- Empty src (result is empty string, skip check to avoid blank-line ambiguity)
-- @skip_result_check=true
USE ${case_db};
SELECT TRANSLATE('', 'CS', '测试');

-- query 43
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 44
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('abc', 'ab', ''),
    ('', 'CS', '测试');

-- query 45
-- Empty string handling: table-based (results contain empty strings, skip check)
-- @skip_result_check=true
USE ${case_db};
SELECT TRANSLATE(src, from_str, to_str) from t1;

-- query 46
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES ('abc', '', 'ab');

-- query 47
-- Empty from_str among other rows (results contain empty strings, skip check)
-- @skip_result_check=true
USE ${case_db};
SELECT TRANSLATE(src, from_str, to_str) from t1;

------------------------------------------------------------------------
-- Section 5: test_translate_null
------------------------------------------------------------------------

-- query 48
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 49
-- NULL to_str
USE ${case_db};
SELECT TRANSLATE('abc', 'ab', NULL);

-- query 50
-- NULL from_str
USE ${case_db};
SELECT TRANSLATE('abc', NULL, 'ab');

-- query 51
-- NULL src
USE ${case_db};
SELECT TRANSLATE(NULL, 'CS', '测试');

-- query 52
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 53
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('abc', 'ab', NULL),
    (NULL, 'CS', '测试'),
    ('abc', null, 'ab');

-- query 54
-- NULL handling: table-based
USE ${case_db};
SELECT TRANSLATE(src, from_str, to_str) from t1;

------------------------------------------------------------------------
-- Section 6: test_translate_invalid
------------------------------------------------------------------------

-- query 55
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 56
-- Invalid: TRANSLATE with 2 args (constant)
-- @expect_error=No matching function
USE ${case_db};
SELECT TRANSLATE('abc', 'ab');

-- query 57
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 58
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('abc', 'ab', NULL),
    (NULL, 'CS', '测试'),
    ('abc', null, 'ab');

-- query 59
-- Invalid: TRANSLATE with 2 args (table-based)
-- @expect_error=No matching function
USE ${case_db};
SELECT TRANSLATE(src, from_str) from t1;

------------------------------------------------------------------------
-- Section 7: test_translate_random
------------------------------------------------------------------------

-- query 60
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 61
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 62
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('ba1+E^]-', NULL, 'EE1aaababb'),
    ('(2%&j四[iH%û一G*%ù😂cûg˜', '%2(û*û😍😂在四', 'HjiiiG22'),
    ('🙂', 'K🙂🙂🙂🙂🙂🙂测🙂🙂🙂', '0wCnAcyybBK'),
    ('八*二四试DA%$aF[?^â%一›*Ji›a七+(cJ五^😄D三gûæ五', 'VFaiAgAJFcDu', 'DFJAFgF'),
    ('E?jjj?FE4二&3#$]æ#七二#eh三B*e^试4B˜E一(E', 'B*›æe?&', '七#eE*A?d'),
    ('*›Jù2âd0g-ga(#[di八%七🙂四%aH九âHcA--五I0›', 'i%-0-›cg四q0*eJ', 'Hcgk4gisgHHaaa'),
    ('ûbe', 'ûûbeeûBeûb', 'bûege在beQeeVx'),
    ('七0三$ei+DBgi测ùc(C试i˜试#)0+😄^)😄]e😇c4F八1G)三七ùHg🙂', 'K34Fc2E44e', 'B1e4HFgJDw'),
    (')g六BæùhähI)JB五j二', 'hJæ😍五âCBIBJ', 'JBDhBgvBI9Jeh'),
    ('HDa$四0d八测%😇æ)J一c?0)2测九jdaûf›二Câ', '九测)😇九aâ', 'a0dcad23DJKb'),
    ('Id二?-😇ba二Bg一f九E测E一(😇C测3JB?^2j😇A九&[+3JCh😂五五›', '😇J😇一-[Ej', 'Eb(+hg二'),
    ('九HB2😄六D🙂C七试三D五d3?D😇JùFA3I^?2#fa😇试😇七›dJ(b二iDb', 'F三f😇DJ试六xA2不J九', 'bobJAD3IH'),
    ('六G3一🙂a#', 'aG3G3F3a34', 'boaG3G'),
    ('七四c*û😂0七›测ââ2[三g]f*五Eä[Di测ù&2ä八-一', '四iâYNEù2', '20cC0cW2iE'),
    ('[h试û]八3六五i4Hb1äJ🙂😇🙂DB六🙂Efa八$f*0四i]Hâj1二G?一?I测😂', 'Hfh3r43JEnhD9I', 'fwhIGb'),
    ('1ù)一fC🙂G🙂Bd*😂^C$AI]]j?23hH˜?😄E)Cb1Ah4D3%😇三🙂]*â二û*', 'CIyjA31zGIZ', NULL),
    ('*试Jû😇三😇Ea四一+æ🙂G$F+›二八+🙂🙂😂😄cA#a#?c&äI四f˜iiæ试›ej😄🙂c', 'tceTni55ie', 'AGAFceF'),
    ('g-$一A˜g七afD+😂)一e^]ù😇Ah五D😇G*jI+试二G四#😇八#F&[2$0', 'WGuVZ4vG2DDDg', 'g0hAhe'),
    ('-D&i😇😇七Bù?#gä[G&😇', NULL, 'ä😇?😇&测ù不&D'),
    ('🙂四dc测äF^$))g4B😇äd4gG^eAaF😇九4G[I$]›Ib八-0七五五+😂#', '4Gbdg44IIAFgd', 'cSdIbqBHeAGBb'),
    ('😇😄试九äæ1七]A[J🙂七^五2g六😄ûû?3jf1e4?a😂ù🙂一Ed😇BfäI[j😇0e', 'g4LI😍J😇ä1😄🙂😂测a', 'f1测f😇û'),
    ('-F%âI4🙂六af', '六âqF%âII', '34jQ8IIIII'),
    ('A三e%😂›+(*三A试&一h%dä一ä&^cæj🙂五››ha(Câ四*Gû]#a??😇', 'd›h一😄ä›2', 'AG9ZjGIAePda'),
    ('D一#)', 'DsDDgDDbJDDZdD', 'zDDtDDD'),
    (NULL, '不😍1U˜试Ni', '9JLv在试😍0V😂˜x'),
    ('😂â😇2)#?1aA2ä五?测Gf˜æ%2%😇', 'GeG2Cf21a', 'G122pid2'),
    ('😄#2%六B😄[3)*gæ(A)试A[ä˜五^*😂三?😂二aF?äC八û0aI[F测4六æ&1', 'F14B1a', 'FFI52FAFAF10ag'),
    ('%›1Ga😂', 'NGGb11', 'aGaaa1'),
    ('(五û试%H试GH三ä四九3ah😇三(&âbâFib(Bâä试Iai[九4六3)â', '六âH试不âa三˜Bû(九九', 'âK(iFG[GH六Bi😇'),
    (NULL, NULL, 'QJngp57'),
    ('😂[六›˜&六三^]âE2A四0âb二JFf?û[六3^-d🙂˜dä', 'd六û˜câ😂d-六s[', 'E3s03yd'),
    ('+', 'BnloXjep1Vosfz', 'asJO5T'),
    ('#bC3*六八?**J试a七)äBi😇E试d😂]五九(˜)-E)😇六i4+一-Cd]2九七', 'diCdJEB', '›4a九测˜-在-b'),
    ('1[a-˜Fbiæj', 'ba1b1j', 'VFâF˜1j[b'),
    ('测G', 'GGEGGG', '😍qGn测测G测GG6不G'),
    ('*j)😂(F-B#â+😇d3测H#三五试(ù0e+f八七[äI#f˜G4A#', '63O0fnds3', 'IffFddRFjI34'),
    ('六c八0)GâûFgCä一i›gF', 'iCFcHGd', 'iCwcGgFggP'),
    ('f😂三-&â)J4', 'hJffJfIBfJJ', '-â&â不试e)&'),
    ('三e*FaE$4e%测?C#二JæHc]四六+)九B˜四æj››😇😄一gIfgdDG七ù4', '4😂g😄R$不D', NULL),
    ('九g九%😂(0九b七›j)fi试F😇八😄[一0ùE›F八3ûûhc3测八+äe', 'a%Xnf😂', 'hF0giZE0e3'),
    ('e二d*D%4eäcæ*û😂*', '在ä测û在在eBb8e*æ', 'e4cc4Ddge'),
    (NULL, '8VjLoB8', 'bDNPgO3f'),
    ('Câ测六三A😇😂Ggc一+1›f21j2%f😇â五æaBj*-#C六C', '1AcGcB1QICBjG', 'g😇j😂›GH˜六o'),
    ('hE九H一cC测测cj2%', 'EEEcC1cczmHr', 'jEHHhDj'),
    ('', 'jqIPRsQc', '3S4KSpB'),
    ('bH二ùD八J😂j2b八2😂B-ghä#&😂D+Hdûf😂e测', 'hBDE1HSjJHHJj', 'U2bjmbb8dhI2'),
    ('[😇二0C试ä', '01CC00CCaC', '0CHCCC0CC0CCvC'),
    ('😄D1˜九C八0六三jea-e3&e试e', 'et2J03G', 'eUok00jCDDGa'),
    (NULL, '›Y测l😜fâN', 'D不âmâ6e😜'),
    ('#]九â*JCg)jû^äHùAi四Jf#3', 'ACfgj3jhUC', '#ä)ä九J'),
    ('?hA七🙂HI二#0试😇(四七ùI+a˜ä]二fEb九H12', '2I20hAbOEF', 'hHGI4TAaH'),
    ('Fc˜D4二八Fj😂4i三2😄六二四E34h%g九%D24˜&七Bdedjj😂😂a1试二', NULL, 'v3x%2😂437试&D'),
    ('%I八五四JfJ六&Gc测E3D七+H1e0#Jâäe', 'eNJfgIHEJ', 'ä测0I3JJ七四'),
    ('六#i)˜äigù2DC32c1â六3gd九J3b五$1😂1😇🙂3九æBd^七J%a', '^)S2bd', '33B29五A九dC'),
    ('fE六fb二)I七3?[*😂0G#JA🙂BG3J五测测', 'IfEJGEJJBJbG3b', '?*3J😂二G'),
    ('0😄c-三g😂E2â+-)😂â二#C', 'ä-c2😂˜OCâm', '二â#😂#2-g😂a2三'),
    ('aäAgCæ4一˜😂B4测3æg-3û1三æ›H#?', '43A4HAHa3', '1三?-1F›测一g一'),
    ('If测七Fi4ùFc', 'mIfif4FiFF测i😂', 'fFFF4iaI'),
    ('😄H[五A+试1c🙂J七I六bä+E😇[', '1HwccAJ1EJo', 'HbAbAA'),
    ('æ三测ûfi][)42û˜jd3i1i?gcI😂(fD[e?FI', 'Icddie', 'ibfgfiij3Dfbi'),
    ('D*^D八😂1😄测H4八Df九ebe4', '😄f*fâDDf😂›😂九f', '4D14s1eH6D4b'),
    ('八c-0e九g3二二a二i八igJ七e', 'kJcJ二e二ig二m', 'caaMgigdiyJF'),
    ('D#GD^4九试cE[八?-j0F03D', 'w试-DE-pb-[E试9#', '03LDF3D'),
    ('三%-&一û二F', 'FFPFFFFF', 'FFFFFsQw91'),
    ('Ji三æG0(0dùûh六[G^七3]八2', 'Ge0i0Fn0h3', 'GGG303'),
    ('jC*八^八六ce›a(GâB', 'cC1GeePGC', 'a6GjcC'),
    ('三3一四â4˜', '43G3333344I4', '😍四一˜ââ34一˜😜3'),
    ('八😄hbEdG一H?😇gFù🙂🙂+1û*五æ(😄D', 'EGgdZbFyEbE', 'GQgX1hFEghDHE'),
    ('😂FFj🙂D%D?一ùùûä0fi2ba(A#a*fû', 'bAffaojiDfU2i7', '?m2˜%😍D0anâfù'),
    ('3*i八I八(五j-🙂?', 'W八😄?八IIi?八i🙂', '3ULIIPu'),
    ('fhI二hIf˜âaC三', 'fC˜˜I二˜IhaGq', '😜˜Ehâa三hä'),
    ('Gæj1^', 'G1jGy111j', 'j3QG1jG'),
    ('😂0C%🙂$?äi😄六h三测^🙂0Eù0))æIæ›h0i七i-4)3B›E#4😄', ')hæh😄😄BEæ?', 'ih4Ei0I0'),
    ('˜四I*CEg1🙂]+', 'CCCgIgIC', 'EsECCIC2111'),
    ('c0C😇0😄', NULL, '0CJp0c00V0'),
    ('4%?j测[])0FGBg]六九db九i*四四I😄九😄', 'oZ测九Q九B', 'biGFVbRbgB'),
    ('äda1五4$&0I#😄😄ä五$试测$🙂I测æG七›æ😄😇', 'IIIIG11pdG1', 'dG1ddd'),
    ('^]?测äc🙂e七dbcû😄f4â😇+*I)+û测Bd八😇四1', '4beB1c1IdfIQ1b', 'bbdRIf'),
    ('三g二😄测[-😇*', 'gggHggggogg', 'ggggIgrgggg'),
    ('2IæIGj😇[(^I+G一B*c-hâ九😇00八jC', 'CCGxhI', 'cGG😜J˜It0在j一'),
    ('4›ää0😄2测IJDe', 'ZDo4D4e', '😄😂不😄I›😄4'),
    ('jj(f七[e', '不😜j七IAjFe七j(ej', 'jjjjfefjjfffew'),
    ('h$3%-CBæDeG八)›#三#I#&gc😇六Ii$八-+e', 'D-#+😂#DI😜âæd', 'h7eBihgcDjgB'),
    ('BC七四?i-Bd3]', 'BC38di', 'B33B3BBdC3Y'),
    ('F(五#›H[七%H%四一九D›3😇e-😂九)1二D&ä', '😇1yF#九)(😂', 'UFR五😂C[二'),
    ('DjHGJ˜dBûa˜试H)&#d一â›0j›D', 'Hl试D)››H', NULL),
    ('C*#测›hâFc', 'Câ测Fc测oV›', 'CchFChchFhDcfF'),
    ('六*六bæ九gg[&三🙂(4C&H1^3›^d˜D%ù六测试4E一)六0ù', 'H[D4😂J三一六在九˜试', 'b4g1DggCp04'),
    ('🙂一八C五C', 'CrCCCC', 'ClCCCC'),
    ('eæE一û+I]3八ä试四?七试jfFBGB0â三四', '5EGo09En', '八++?FbG]一😂四WEf'),
    ('â😄0Fû', 'p0FFAF', 'D0â😄00a'),
    ('六🙂一测Bfcj0AaC九😇D七gG[1^八1九GCAe1æiB😇(🙂+D😄六c', 'ioY1cgD', '九B1G一c'),
    ('H五G六d试›ca(+?gC^ù$g[三e&Aj四😄ä', 'b五六四?03H+😄›三+', 'jHCSjAeecd1BH'),
    ('[$aâj二😄C]?ù八ûù[试æ˜四', 'ajDCgajaaj', 'û˜测d?â测æùj试'),
    ('GCcf+i五31🙂🙂八?)˜˜d😇$九试ûe0+æ1🙂#', '1aG🙂?)', 'C˜E🙂äCCB不$五🙂试'),
    ('^›测#]J)ihâI九2九八2^J-)˜3四1😇1B1g二&B二gj', 'lggjB2j63gh', '))˜1B3]J-ji&J'),
    (NULL, '˜q在不GN在X测', '˜在âMBy😄6'),
    ('', 'QTL3ND', 'UTs1QEhpnf'),
    ('iEJG2A六0AB#(', 'BE六AAGG(六', 'EiGAi0GigA'),
    (')i-五二+$Bc+六â&三四^eG*七1ûcæ0äF?J八(*六D四五û九%CfJ*C九â', 'J四%O(测三B0', NULL);

-- query 63
-- Random data: all non-constant args aggregate
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, from_str, to_str))), 0) FROM t1;

-- query 64
-- Random data: constant from_str with non-constant to_str
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'ù七[I%10', to_str))), 0) from t1;

-- query 65
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'ù七[I%10', NULL))), 0) FROM t1;

-- query 66
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'ù七[I%10', '1d44cdQjCX'))), 0) FROM t1;

-- query 67
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'ù七[I%10', 'hdA4ZE'))), 0) FROM t1;

-- query 68
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'ù七[I%10', '4AlJzF'))), 0) FROM t1;

-- query 69
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'AaPWie', to_str))), 0) from t1;

-- query 70
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'AaPWie', NULL))), 0) FROM t1;

-- query 71
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'AaPWie', '1d44cdQjCX'))), 0) FROM t1;

-- query 72
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'D不b06&😄0IF😂˜', to_str))), 0) from t1;

-- query 73
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'D不b06&😄0IF😂˜', '1d44cdQjCX'))), 0) FROM t1;

-- query 74
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'D不b06&😄0IF😂˜', 'hdA4ZE'))), 0) FROM t1;

-- query 75
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'Jf😄dJeCc😂f六˜', to_str))), 0) from t1;

-- query 76
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'Jf😄dJeCc😂f六˜', NULL))), 0) FROM t1;

-- query 77
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, 'Jf😄dJeCc😂f六˜', '4AlJzF'))), 0) FROM t1;

-- query 78
-- Random data: non-constant from_str with constant to_str
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, from_str, '1d44cdQjCX'))), 0) FROM t1;

-- query 79
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, NULL, '1d44cdQjCX'))), 0) FROM t1;

-- query 80
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, from_str, 'hdA4ZE'))), 0) FROM t1;

-- query 81
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, NULL, 'hdA4ZE'))), 0) FROM t1;

-- query 82
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, from_str, NULL))), 0) FROM t1;

-- query 83
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, NULL, NULL))), 0) FROM t1;

-- query 84
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, from_str, '4AlJzF'))), 0) FROM t1;

-- query 85
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, NULL, '4AlJzF'))), 0) FROM t1;

-- query 86
-- Random data: constant src, non-constant from_str and to_str (representative subset)
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('4*一测1-F六g五八G八😇I测iJ😂id3-b二j(试&九e^二h&H二cæB%AæB]g0六[', from_str, to_str))), 0) from t1;

-- query 87
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('æ˜三3h(DIä1j七F试a四G1I›43e›🙂GA二˜##ac五HjD😄jä', from_str, to_str))), 0) from t1;

-- query 88
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('[˜一Gi', from_str, to_str))), 0) from t1;

-- query 89
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('e四i)1😄eJD^*C2ù-Fj]测七', from_str, to_str))), 0) from t1;

-- query 90
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('^😂2æ六d]iBdcc五4?a(😂B1七八1[DC三四])(E0æIæ八一', from_str, to_str))), 0) from t1;

-- query 91
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('', from_str, to_str))), 0) from t1;

-- query 92
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(NULL, from_str, to_str))), 0) from t1;

-- query 93
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('›gH六🙂九›-一B#二EC(', from_str, to_str))), 0) from t1;

-- query 94
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('f^ä?]GhB三˜&Hb]A😂七七æ六?', from_str, to_str))), 0) from t1;

-- query 95
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('J😇D]&4', from_str, to_str))), 0) from t1;

-- query 96
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('试ù^d二八0', from_str, to_str))), 0) from t1;

-- query 97
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('四1h', from_str, to_str))), 0) from t1;

-- query 98
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('0', from_str, to_str))), 0) from t1;

-- query 99
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE('七', from_str, to_str))), 0) from t1;

-- query 100
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(']一', from_str, to_str))), 0) from t1;

-- query 101
-- Random data: constant src, from_str, to_str (representative subset)
USE ${case_db};
SELECT TRANSLATE('f›˜IGi?hB?😄二E[-E$›)😂›f)0六2e)八2d测Ih四]四c九DFa^', ')0f)›Z', 'ldbcEEDFIG');

-- query 102
USE ${case_db};
SELECT TRANSLATE('3bh$^)g(G八?八-cd4一I3D三cFe二#AICâ😄cB#五ää', '在4A7三g', 'tbDbkc');

-- query 103
USE ${case_db};
SELECT TRANSLATE('七ca&+âDù三˜六九#ûc+%六h$›B测æ四🙂七â*1*', NULL, 'a1B1DhDhgDc');

-- query 104
USE ${case_db};
SELECT TRANSLATE('hC😇😇3?', 'hCCChh5C3DH3y', '3ChC03Chl3h3');

-- query 105
USE ${case_db};
SELECT TRANSLATE('试&4😂', 'T4😂😂&试t&&试4', '4&&试ä&试xHZ&K4');

-- query 106
USE ${case_db};
SELECT TRANSLATE('-二c%🙂G九c%^#C-^h四›九0三', 'b›不%G试0九cd二cc', '3GCChhc');

-- query 107
USE ${case_db};
SELECT TRANSLATE('九Bb-jâC2三Eû🙂F*)0😄æEbjû四八九😂d*â˜D1ceJ🙂›八+-32ùG˜试4', 'bbbF1DI', NULL);

-- query 108
USE ${case_db};
SELECT TRANSLATE('?h*I3hD›j测八â]fG1🙂GBB4试dijæ0gg1😄F🙂â四三&e*[九', NULL, '›âx3&B');

-- query 109
USE ${case_db};
SELECT TRANSLATE('æ˜😂😂gD›E测JE一˜3😄i四五äF3^ùHâ)九geHF😄˜1h五&I#a', 'MFivH3', 'ggFUEEa3');

-- query 110
USE ${case_db};
SELECT TRANSLATE(')AI😂#æ?Fûg2äDû试九', 'gggwA2uVFUd3D', 'Y4RFIx2I');

-- query 111
USE ${case_db};
SELECT TRANSLATE(NULL, 'USYiLqixr', '7iext05GARPW4A');

-- query 112
USE ${case_db};
SELECT TRANSLATE('ie4eC[g-2九*I😄八jfI😄八)a九âäE🙂4?1一æ六😄4🙂ù四3九3j一?', 'Jg😍9😂😍I一?😄ùC', '3Najg2ei44I');

-- query 113
USE ${case_db};
SELECT TRANSLATE('E2#›&I+%', 'I2EIE2C2222II', '试I😄%›E+EI😍测&');

-- query 114
USE ${case_db};
SELECT TRANSLATE('(Fd$bFfäF五九二Ij', 'FFNj九bd试jf九ä', NULL);

-- query 115
USE ${case_db};
SELECT TRANSLATE(']3😇😂二五I+IGa##E八Hæah', '二五#Ea]', 'caIhVya3HIIfGJ');

-- query 116
USE ${case_db};
SELECT TRANSLATE('六eJH五C九^', 'eAYeeCJH', '^五e九九C');

-- query 117
USE ${case_db};
SELECT TRANSLATE('六😄1C›+eJ二%â七😂H[]›aHù一', 'HoaAmFe1JN', NULL);

-- query 118
USE ${case_db};
SELECT TRANSLATE(NULL, 'bCb0r0NTh', 'DJBQcsGL99EDOU');

-- query 119
USE ${case_db};
SELECT TRANSLATE('😄4jCdha九?😇3H]Fef4试八hgF五五J三c)🙂七2%hF🙂a-If😂a›[#j˜J', '4cxIhufm3hn', '5GxFgChdkHdIa');

-- query 120
USE ${case_db};
SELECT TRANSLATE('h', 'hhzhhhhhChFh', 'hhhh8hhh');

-- query 121
-- Permutation: from_str and to_str with various lengths (deduplicated)
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '˜â测😄a试'))), 0) from t1;

-- query 122
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '˜试'))), 0) from t1;

-- query 123
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '˜âa试'))), 0) from t1;

-- query 124
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '˜a'))), 0) from t1;

-- query 125
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', 'â测😄a'))), 0) from t1;

-- query 126
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', 'â测试'))), 0) from t1;

-- query 127
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '测😄a试'))), 0) from t1;

-- query 128
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '测😄'))), 0) from t1;

-- query 129
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '˜😄试'))), 0) from t1;

-- query 130
USE ${case_db};
SELECT ifnull(sum(murmur_hash3_32(TRANSLATE(src, '˜›âä测试😄😂ab', '˜测😄'))), 0) from t1;

------------------------------------------------------------------------
-- Section 8: test_translate_utf8_constant_from_and_to
------------------------------------------------------------------------

-- query 131
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '测');

-- query 132
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', 'b');

-- query 133
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', 'b测');

-- query 134
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '😂');

-- query 135
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '😂测');

-- query 136
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '😂b');

-- query 137
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '😂b测');

-- query 138
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '试');

-- query 139
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '试测');

-- query 140
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测😄', '试b');

-- query 141
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '测');

-- query 142
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', 'b');

-- query 143
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', 'b测');

-- query 144
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '😂');

-- query 145
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '😂测');

-- query 146
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '😂b');

-- query 147
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '😂b测');

-- query 148
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '试');

-- query 149
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '试测');

-- query 150
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '测试', '试b');

-- query 151
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '测');

-- query 152
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', 'b');

-- query 153
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', 'b测');

-- query 154
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '😂');

-- query 155
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '😂测');

-- query 156
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '😂b');

-- query 157
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '😂b测');

-- query 158
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '试');

-- query 159
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '试测');

-- query 160
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â😄', '试b');

-- query 161
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '测');

-- query 162
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', 'b');

-- query 163
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', 'b测');

-- query 164
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '😂');

-- query 165
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '😂测');

-- query 166
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '😂b');

-- query 167
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '😂b测');

-- query 168
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '试');

-- query 169
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '试测');

-- query 170
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'âa试', '试b');

-- query 171
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '测');

-- query 172
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', 'b');

-- query 173
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', 'b测');

-- query 174
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '😂');

-- query 175
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '😂测');

-- query 176
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '😂b');

-- query 177
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '😂b测');

-- query 178
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '试');

-- query 179
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '试测');

-- query 180
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜试', '试b');

-- query 181
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '测');

-- query 182
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', 'b');

-- query 183
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', 'b测');

-- query 184
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '😂');

-- query 185
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '😂测');

-- query 186
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '😂b');

-- query 187
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '😂b测');

-- query 188
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '试');

-- query 189
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '试测');

-- query 190
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '˜â测', '试b');

-- query 191
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '测');

-- query 192
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', 'b');

-- query 193
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', 'b测');

-- query 194
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '😂');

-- query 195
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '😂测');

-- query 196
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '😂b');

-- query 197
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '😂b测');

-- query 198
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '试');

-- query 199
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '试测');

-- query 200
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â', '试b');

-- query 201
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '测');

-- query 202
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', 'b');

-- query 203
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', 'b测');

-- query 204
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '😂');

-- query 205
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '😂测');

-- query 206
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '😂b');

-- query 207
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '😂b测');

-- query 208
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '试');

-- query 209
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '试测');

-- query 210
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'â测a试', '试b');

-- query 211
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '测');

-- query 212
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', 'b');

-- query 213
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', 'b测');

-- query 214
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '😂');

-- query 215
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '😂测');

-- query 216
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '😂b');

-- query 217
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '😂b测');

-- query 218
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '试');

-- query 219
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '试测');

-- query 220
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', '😄a试', '试b');

-- query 221
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '测');

-- query 222
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', 'b');

-- query 223
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', 'b测');

-- query 224
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '😂');

-- query 225
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '😂测');

-- query 226
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '😂b');

-- query 227
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '😂b测');

-- query 228
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '试');

-- query 229
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '试测');

-- query 230
USE ${case_db};
SELECT TRANSLATE('˜›âä测试😄😂ab', 'a', '试b');
