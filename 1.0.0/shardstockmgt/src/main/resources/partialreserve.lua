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
  local curoversell = redis.call('HGET', curlocation, curoversellkey)
  local curinstockval = 0
  if curinstock then 
    curinstockval = tonumber(curinstock)
  end
  local curreservedval = 0 	
  if curreserved then
    curreservedval = tonumber(curreserved)
  end 
  local curoversellval = 0 	
  if curoversell then
    curoversellval = tonumber(curoversell)
  end  
  local curavailable = curinstockval + curoversellval - curreservedval;
  if (curavailable - quantities[i] < 0) then 
  	local curresline = {}
    redis.call('HINCRBY', curlocation, curreservedkey, curavailable)
	table.insert(curresline, curlocation)
	table.insert(curresline, curprod)
	table.insert(curresline, (quantities[i] - curavailable))
	table.insert(results, curresline)
  else 
    redis.call('HINCRBY', curlocation, curreservedkey, quantities[i])  
  end
end
return results