local prodcodes = {}
local locationIds = {}
local quantities = {}
local curreservedvals = {} 
local curlocation
local curprod
local success = 1
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
     success = 0
	 break
  end
end
if success == 1 then
  for i=1,table.getn(prodcodes) do
    local curinstockkey = prodcodes[i] .. "_i"
    local curreservedkey = prodcodes[i] .. "_r"
	redis.call('HINCRBY', locationIds[i], curinstockkey, -quantities[i])
	redis.call('HINCRBY', locationIds[i], curreservedkey, -quantities[i])
  end
end  
return success
