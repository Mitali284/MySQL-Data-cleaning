SELECT * FROM world_layoff.layoffs;


-- 1. Remove Duplicates 
-- 2. Standardize the data 
-- 3. Null values or blanks
-- 4. Remove any columns or Rows


CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM layoffs_staging;
 

SELECT *,
row_number() over(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) As row_no
 FROM layoffs_staging;
 

WITH duplicate_cte AS (
    SELECT *,
    row_number() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_no
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_no > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper'

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

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
    SELECT *,
    row_number() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_no
    FROM layoffs_staging;
 
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

delete 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

SELECT * 
FROM layoffs_staging2;

-- Standardizing the data
 
SELECT company, trim(company)
from layoffs_staging2;

UPDATE layoffs_staging 
SET company = trim(company);

SELECT DISTINCT industry
from layoffs_staging2
order by 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
from layoffs_staging2

SELECT DISTINCT location
from layoffs_staging2
order by 1;


SELECT DISTINCT country
from layoffs_staging2
order by 1;

SELECT *
from layoffs_staging2
where country like 'united States'
order by 1;

SELECT DISTINCT country, trim(trailing '.' FROM country)
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = trim(trailing '.' FROM country)
WHERE country LIKE  'United States%';

SELECT distinct country 
FROM layoffs_staging2
order by 1;

SELECT `date`
FROM layoffs_staging2

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

 
UPDATE layoffs_staging2
SET `date` = `date`
WHERE STR_TO_DATE(`date`, '%Y-%m-%d') IS NOT NULL;

ALTER table layoffs_staging2
modify column `date` DATE;

SELECT * FROM
layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is null ;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT * 
FROM layoffs_staging2
Where industry is null
or industry = '';



SELECT *
 FROM layoffs_staging2
 WHERE company  LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company = t2.company
 AND t1.location = t2.location
 WHERE (t1.industry IS NULL or t1.industry = '' )
 AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
 AND t2.industry IS NOT NULL;
 
 SELECT * FROM
layoffs_staging2;
 
 SELECT * FROM
layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is null ;

DELETE 
 FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is null ;

 SELECT * FROM
layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP column row_num;












 







