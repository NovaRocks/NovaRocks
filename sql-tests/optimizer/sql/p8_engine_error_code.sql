-- @tags=optimizer,p8,engine_error_code
-- @expect_error_code=UnsupportedDistributedDmlShape
ADMIN RAISE ENGINE ERROR 'UnsupportedDistributedDmlShape';
