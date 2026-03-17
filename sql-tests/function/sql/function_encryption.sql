-- Migrated from dev/test/sql/test_function/T/test_encryption
-- Test Objective:
-- 1. Validate AES_ENCRYPT/AES_DECRYPT with default mode (AES_128_ECB).
-- 2. Validate AES_ENCRYPT/AES_DECRYPT across all supported modes: ECB, CBC, CFB, CFB1, CFB8, CFB128, OFB, CTR, GCM.
-- 3. Validate AES_ENCRYPT/AES_DECRYPT with key sizes 128, 192, 256.
-- 4. Validate NULL IV handling (returns NULL for modes requiring IV, except ECB).
-- 5. Validate 5-arg form with AAD is rejected for non-GCM modes but accepted for GCM modes.

-- query 1
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3'));

-- query 2
select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA=='),'F3229A0B371ED2D9441B830D21A390C3');

-- query 3
-- @expect_error=requires GCM mode to use AAD parameter
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_ECB", "test"));

-- query 4
-- @expect_error=requires GCM mode to use AAD parameter
select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA=='),'F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_ECB", "test");

-- query 5
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_ECB"));

-- query 6
select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA=='),'F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_ECB");

-- query 7
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_ECB"));

-- query 8
select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA=='),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_ECB");

-- query 9
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_192_ECB"));

-- query 10
select AES_DECRYPT(from_base64('xaf+kW/d5YPRSYFGnFkOfQ=='),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_192_ECB");

-- query 11
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_256_ECB"));

-- query 12
select AES_DECRYPT(from_base64('o8B7eV7n6BqEuO7N453Y4A=='),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_256_ECB");

-- query 13
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CBC"));

-- query 14
select AES_DECRYPT(from_base64('ne2YHEKECQSni3/kRI/nBg=='),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CBC");

-- query 15
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_CBC"));

-- query 16
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CBC"));

-- query 17
select AES_DECRYPT(from_base64('taXlwIvir9yff94F5Uv/KA=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CBC");

-- query 18
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CBC"));

-- query 19
select AES_DECRYPT(from_base64('QK83BHrWHqCWD1mA1u7WGQ=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CBC");

-- query 20
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CBC"));

-- query 21
select AES_DECRYPT(from_base64('4w0ceQx2FqG8GGTivC0QPg=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CBC");

-- query 22
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB"));

-- query 23
select AES_DECRYPT(from_base64('3wGmUeFO4/Ex'),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB");

-- query 24
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_CFB"));

-- query 25
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB"));

-- query 26
select AES_DECRYPT(from_base64('POcBNCCAYPCc'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB");

-- query 27
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB"));

-- query 28
select AES_DECRYPT(from_base64('Ma7ozVBB4lGP'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB");

-- query 29
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB"));

-- query 30
select AES_DECRYPT(from_base64('p6+axkiBSNf2'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB");

-- query 31
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB1"));

-- query 32
select AES_DECRYPT(from_base64('2NB6ZTf15qlj'),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB1");

-- query 33
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_CFB1"));

-- query 34
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB1"));

-- query 35
select AES_DECRYPT(from_base64('JrWt4rBZ2JRl'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB1");

-- query 36
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB1"));

-- query 37
select AES_DECRYPT(from_base64('MltCAryodTud'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB1");

-- query 38
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB1"));

-- query 39
select AES_DECRYPT(from_base64('h5rRJ9HmkeqM'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB1");

-- query 40
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB8"));

-- query 41
select AES_DECRYPT(from_base64('3yAMoqwMEKHp'),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB8");

-- query 42
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_CFB8"));

-- query 43
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB8"));

-- query 44
select AES_DECRYPT(from_base64('PLbtNmFObZU6'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB8");

-- query 45
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB8"));

-- query 46
select AES_DECRYPT(from_base64('MS8Gi0HaPkf8'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB8");

-- query 47
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB8"));

-- query 48
select AES_DECRYPT(from_base64('p8NH5PX2mufr'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB8");

-- query 49
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB128"));

-- query 50
select AES_DECRYPT(from_base64('3wGmUeFO4/Ex'),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CFB128");

-- query 51
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_CFB128"));

-- query 52
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB128"));

-- query 53
select AES_DECRYPT(from_base64('POcBNCCAYPCc'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CFB128");

-- query 54
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB128"));

-- query 55
select AES_DECRYPT(from_base64('Ma7ozVBB4lGP'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CFB128");

-- query 56
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB128"));

-- query 57
select AES_DECRYPT(from_base64('p6+axkiBSNf2'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CFB128");

-- query 58
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_OFB"));

-- query 59
select AES_DECRYPT(from_base64('3wGmUeFO4/Ex'),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_OFB");

-- query 60
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_OFB"));

-- query 61
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_OFB"));

-- query 62
select AES_DECRYPT(from_base64('POcBNCCAYPCc'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_OFB");

-- query 63
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_OFB"));

-- query 64
select AES_DECRYPT(from_base64('Ma7ozVBB4lGP'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_OFB");

-- query 65
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_OFB"));

-- query 66
select AES_DECRYPT(from_base64('p6+axkiBSNf2'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_OFB");

-- query 67
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CTR"));

-- query 68
select AES_DECRYPT(from_base64('3wGmUeFO4/Ex'),'F3229A0B371ED2D9441B830D21A390C3', "", "AES_128_CTR");

-- query 69
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_CTR"));

-- query 70
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CTR"));

-- query 71
select AES_DECRYPT(from_base64('POcBNCCAYPCc'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CTR");

-- query 72
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CTR"));

-- query 73
select AES_DECRYPT(from_base64('Ma7ozVBB4lGP'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_192_CTR");

-- query 74
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CTR"));

-- query 75
select AES_DECRYPT(from_base64('p6+axkiBSNf2'),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_256_CTR");

-- query 76
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_128_GCM", "abcdefg"));

-- query 77
select AES_DECRYPT(from_base64('YWJjZGVmZ2hpamtsdpJC2rnrGmvqKQv/WcoO6NuOCXvUnC8pCw=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_128_GCM", "abcdefg");

-- query 78
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_192_GCM", "abcdefg"));

-- query 79
select AES_DECRYPT(from_base64('YWJjZGVmZ2hpamtsIpxL5blh0/zHNXw5nG/tL68lc1BJZ/fCPA=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_192_GCM", "abcdefg");

-- query 80
select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_256_GCM", "abcdefg"));

-- query 81
select AES_DECRYPT(from_base64('YWJjZGVmZ2hpamts2bRovmWbg/Y5qmz8Lt1L2oebY6lOV92Ozw=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_256_GCM", "abcdefg");
