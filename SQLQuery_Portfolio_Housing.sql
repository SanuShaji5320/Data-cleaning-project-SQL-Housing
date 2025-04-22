select * from nashville
-- STEP 1: Create a working copy of the original dataset

select*  into nashville_staging from Nashville
select* from nashville_staging

-- STEP 2: Standardize the SaleDate format
-- Convert the date to a standard 'date' type

update nashville_staging
set SaleDate = convert(date,SaleDate)

-- STEP 3: Fill in missing PropertyAddress values
-- Identify and fill null PropertyAddress by matching on ParcelID from other rows.

select* from nashville_staging
order by ParcelID

select* from nashville_staging where PropertyAddress is null

-- Use self-join to find matching ParcelIDs with non-null PropertyAddresses

select n1.UniqueID,n1.ParcelID,n1.PropertyAddress,n2.UniqueID,n2.ParcelID,n2.PropertyAddress from nashville_staging n1 join
nashville_staging n2 on n1.ParcelID = n2.ParcelID
where n1.PropertyAddress is null
and n1.UniqueID != n2.UniqueID

-- Update null PropertyAddresses using the matched values from other rows

update n1
set n1.PropertyAddress = n2.PropertyAddress
from nashville_staging n1 join
nashville_staging n2 on n1.ParcelID = n2.ParcelID 
where n1.PropertyAddress is null
and n1.UniqueID != n2.UniqueID

-- STEP 4: Split PropertyAddress into Address and City

select substring (propertyaddress, 1, CHARINDEX(',' , propertyaddress) -1 ) as address,
substring (propertyaddress, CHARINDEX(',' , propertyaddress) +1, len(propertyaddress) ) as city
from nashville_staging

Alter table nashville_staging
add propertysplitaddress nvarchar(255)

Alter table nashville_staging
add propertysplitcity nvarchar(255)

update nashville_staging
set propertysplitaddress = substring (propertyaddress, 1, CHARINDEX(',' , propertyaddress) -1 )

update nashville_staging
set propertysplitcity = substring (propertyaddress, CHARINDEX(',' , propertyaddress) +1, len(propertyaddress) )


select PARSENAME ( replace(owneraddress, ',','.'), 1) as state,
PARSENAME ( replace(owneraddress, ',','.'), 2) as city,
PARSENAME ( replace(owneraddress, ',','.'), 3) as address
from nashville_staging

Alter table nashville_staging
add ownersplitstate nvarchar(255)

Alter table nashville_staging
add ownersplitcity nvarchar(255)

Alter table nashville_staging
add ownersplitaddress nvarchar(255)


update nashville_staging
set ownersplitstate = PARSENAME ( replace(owneraddress, ',','.'), 1)

update nashville_staging
set ownersplitcity = PARSENAME ( replace(owneraddress, ',','.'), 2)

update nashville_staging
set ownersplitaddress = PARSENAME ( replace(owneraddress, ',','.'), 3)

-- STEP 5: Change '0' and '1' in sold as vacant to yes or no

select distinct (soldasvacant) from nashville_staging

alter table nashville_staging
alter column soldasvacant varchar(255)

select soldasvacant,
  case when soldasvacant = '0' then 'No'
	   when soldasvacant = '1' then 'Yes'
	   else soldasvacant
	   end
from nashville_staging

update nashville_staging
set soldasvacant =
case when soldasvacant = '0' then 'No'
	   when soldasvacant = '1' then 'Yes'
	   else soldasvacant
	   end


-- STEP 6: Remove duplicate records
-- Use ROW_NUMBER to flag duplicate rows

with rownumberCTE as (
select* ,
ROW_NUMBER()  over ( 
          partition by 		        
			   parcelID, 
			   SaleDate, 
			   Saleprice,
			   legalreference,
			   propertyaddress  
			    order by 
				  uniqueid) as rownum
from nashville_staging)
select* from rownumberCTE where rownum > 1


with rownumberCTE as (
select* ,
ROW_NUMBER()  over ( 
          partition by 		        
			   parcelID, 
			   SaleDate, 
			   Saleprice,
			   legalreference,
			   propertyaddress  
			    order by 
				  uniqueid) as rownum
from nashville_staging)
delete from rownumberCTE where rownum > 1


-- 7. Deleting un used columns

alter table nashville_staging
drop column owneraddress, taxdistrict, propertyaddress

