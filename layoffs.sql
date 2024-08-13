/*
SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data: Fix spelling error
-- 3. Null and Blank values
-- 4. Remove any Columns


-- create staging table for cleaning and adjustments
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

*/

-- 1. Remove Duplicates
SELECT * FROM layoffs_staging;

-- assign row number to find duplicates
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
-- assign row number to find duplicates
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

-- create another staging table with column row_num and delete rows where row_num > 1

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2
where company = 'Casper';

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
where row_num > 1;

-- disable safe mode from mysql to be able to delete rows 
-- SET sql_safe_updates=0;

-- 2. Standardizing

-- company 
-- blank values haven't been filled
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- location
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- delete the dot using TRAILING
SELECT DISTINCT country, TRIM(TRAILING '.' from country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' from country)
WHERE country LIKE 'United States%';

-- Reformat `date` to date type
SELECT *
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging2;

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = CASE
                WHEN `date` IS NOT NULL AND `date` != 'None' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
                ELSE NULL
            END;

-- Change column type in the discription
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null and Blank values

-- populate industry
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = 'None';

SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET	t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

    
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- total_laid_off + percentage_laid_off
UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = 'None';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'None';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove columns and rows we don't use
-- delete the columns with total_laid_off + percentage_laid_off is null because cant populate data
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Exploratory Data Analysis EDA

SELECT *
FROM layoffs_staging2;

-- total_laid_off
-- change data type of column from string to int

ALTER TABLE layoffs_staging2 ADD COLUMN total_laid_off_int INT AFTER percentage_laid_off;

UPDATE layoffs_staging2
SET total_laid_off_int = CAST(total_laid_off AS UNSIGNED);

SELECT total_laid_off, total_laid_off_int
FROM layoffs_staging2
ORDER BY total_laid_off_int DESC;

ALTER TABLE layoffs_staging2 DROP COLUMN total_laid_off;
ALTER TABLE layoffs_staging2 CHANGE COLUMN total_laid_off_int total_laid_off INT;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- date range starts in 2020 when Covid pandemic started and ends 3 years later in 2023 
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- total laidoffs per company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- = ORDER BY SUM(total_laid_off) DESC;

-- total laidoffs per industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- total laidoffs per country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- total laidoffs per year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- total laidoffs per year-month
SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `month`
HAVING `month` IS NOT NULL
ORDER BY 1 ASC;
-- rolling total_laidoff per year-month
WITH rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY `month`
HAVING `month` IS NOT NULL
ORDER BY 1 ASC
)
SELECT `month`, total_off, SUM(total_off) OVER(ORDER BY `month`) AS rolling_total
FROM rolling_total;


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 1 ASC;

-- rank which 5 country has the most layoffs each year
WITH Company_Year AS (
    SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS (
    SELECT *,
        DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
    FROM Company_Year
    WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;


SELECT *
FROM layoffs_staging2;



























