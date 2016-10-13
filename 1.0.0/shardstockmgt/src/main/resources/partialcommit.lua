local prodcodes = {}
local locationIds = {}
local quantities = {}
local curlocation
local curprod
local results = {}
for token in string.gmatch(ARGV[1], "[^,]+") do
   table.insert(locationIds, token)
end
for token in string.gmatch(ARGV[2], "[^,]+") do
   table.insert(prodcodes, token)
end
for token in string.gmatch(ARGV[3], "[^,]+") do
   table.insert(quantities, tonumber(token))
end
for i=1,table.getn(locationIds) do
  curlocation = locationIds[i]
  curprod = prodcodes[i]
  local curinstockkey = curprod .. "_i"
  local curreservedkey = curprod .. "_r"
  local curoversellkey = curprod .. "_o"
  local curinstock = redis.call('HGET', curlocation, curinstockkey)
  local curreserved = redis.call('HGET', curlocation, curreservedkey)
  local curinstockval = 0
  if curinstock then 
    curinstockval = tonumber(curinstock)
  end
  local curreservedval = 0 	
  if curreserved then
    curreservedval = tonumber(curreserved)
  end 
  if (curinstockval - quantities[i] < 0 or curreservedval - quantities[i] < 0) then 
	local curresline = {}
	local deducted = 0
	local remain = 0
	if curinstockval  < curreservedval then 
		deducted = curinstockval
		remain = curreservedval - curinstockval
	else 
		deducted = curreservedval
		remain = curinstockval - curreservedval
	end 
    redis.call('HINCRBY', curlocation, curreservedkey, -deducted)
    redis.call('HINCRBY', curlocation, curinstockkey, -deducted)
	table.insert(curresline, curlocation)
	table.insert(curresline, curprod)
	table.insert(curresline, remain)
	table.insert(results, curresline)
  else 
    redis.call('HINCRBY', curlocation, curreservedkey, -quantities[i])
	redis.call('HINCRBY', curlocation, curinstockkey, -quantities[i])	
  end
end
return results