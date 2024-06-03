SELECT * FROM layoffs;

SELECT COUNT(*) AS total 
FROM layoffs;

# To disable safe mode:
SET SQL_SAFE_UPDATES = 0;

# Creating copy of the data to protect the original data from changes.
# New changes will be made in copied table
-- 1.Remove Duplicates--
Create table layoffs_staging 
like layoffs;

Select * from layoffs;

Insert layoffs_staging
select * from layoffs;	

Select * from layoffs_staging;


with duplicate_cte as( 
Select *, 
Row_number() Over(
Partition by company, industry, total_laid_off,percentage_laid_off,'date' ) as row_num
from layoffs_staging  
)
select * from duplicate_cte
where row_num > 1;

select * from layoffs_staging
where company ='Oda';


with duplicate_cte as( 
Select *, 
Row_number() Over(
Partition by company, location, industry, total_laid_off,percentage_laid_off,'date', stage , country,
funds_raised_millions ) as row_num
from layoffs_staging  
)
select * from duplicate_cte
where row_num > 1;

select * from layoffs_staging
where company ='Oyster';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
Select *, 
Row_number() Over(
Partition by company, location, industry, total_laid_off,percentage_laid_off,'date', stage , country,
funds_raised_millions ) as row_num
from layoffs_staging;

select * from layoffs_staging2
where row_num >1;

Delete
from layoffs_staging2
where row_num >1;

-- 2. Standradizing the data --
Select company , trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

Select distinct industry 
from layoffs_staging2
order by 1;

select * 
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry from layoffs_staging2;

select distinct location from layoffs_staging2 order by 1;

select distinct country from layoffs_staging2 order by 1;

select distinct country ,  trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country  = trim(trailing '.' from country)
where country like 'United States%';

Select `date` from layoffs_staging2;

Select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;	

Alter table layoffs_staging2
modify column `date` DATE;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;


-- 3.Null values or Blank Values --

update layoffs_staging2
set industry = NULL
where industry ='';

select * from layoffs_staging2
where industry is null 
or industry = '';

select * from layoffs_staging2
where company = 'Airbnb'; 

select t1.industry , t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company 
	and t1.location = t2.location
where (t1.industry is null or t1.industry = '' )
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;
    
    

select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

DELETE from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

-- 4.Remove unwanted columns--

 Alter table layoffs_staging2
 drop column row_num;

select * from layoffs_staging2;

-- EDA -- Exploratory data analysis --

Select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select * 
from layoffs_staging2
where percentage_laid_off =1
order by total_laid_off desc;

select * 
from layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions desc;

select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`) , max(`date`)
from layoffs_staging2;

select industry, company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

select country, sum(total_laid_off) 
from layoffs_staging2
group by country
order by 2 desc;

select *
from layoffs_staging2;

Select  sum(total_laid_off),year(`date`) 
from layoffs_staging2
group by year(`date`)
order by 1 desc;

  
Select company, sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

# Rolling total layoffs month wise

select substring(`date`,1 ,7) as `MONTH`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1 ,7) is not null
group by `MONTH`
order by 1 asc;


With Rolling_total as
(select substring(`date`,1 ,7) as `MONTH`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1 ,7) is not null
group by `MONTH`
order by 1 asc
)
select `MONTH`,total_off,  sum(total_off) over(order by `MONTH`) as rolling_total
from Rolling_total;
  
Select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

Select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year(company , years , total_laid_off) as
(
Select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *, 
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
order by ranking asc;


with company_year(company , years , total_laid_off) as
(
Select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as 
(select *, 
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select * 
from company_year_rank
where ranking <=5;



















