/* 
COVID-19 Data Exploration

Skills used: Joins, CTE's, Temp Tables, Window Functions, Aggregate Functions, Creating Views, Converting Data Types

*/


SELECT *
FROM PortfolioProject..CovidDeaths$
WHERE continent IS NOT NULL
ORDER BY 3,4;



--Select Data that we are going to be starting with

SELECT 
	location,
	date,
	total_cases,
	new_cases,
	total_deaths,
	population
FROM PortfolioProject..CovidDeaths$
WHERE continent IS NOT NULL
ORDER BY 1,2;

--  Total Cases vs. Total Deaths.
/* Looks at how many cases do they have in the country and how many deaths do they have for their 
entire group of cases. */

--Shows likelyhood of dying if you contract covid in your country 

SELECT 
	location,
	date,
	total_cases,
	total_deaths,
	(total_deaths/total_cases)*100 AS DeathPercentage
FROM PortfolioProject..CovidDeaths$
WHERE location like '%states%'
AND continent IS NOT NULL
ORDER BY 1,2;


SELECT
    location,
    date,
    total_cases,
    total_deaths,
    (CAST(total_deaths AS FLOAT) / NULLIF(total_cases, 0)) * 100 AS case_fatality_rate
FROM PortfolioProject..CovidDeaths$ 
WHERE location like '%states%'
AND continent IS NOT NULL
ORDER BY location, date;




/* What if i want it to not take nulls into account. 
That way, it doesn't consider total_cases and total_deaths as NULL. 
I just want the result to not have nulls, what do i do ? */

-- OPTION 1:
/*🔹 1. Filter them out with WHERE

This removes rows where either total_cases or total_deaths is NULL:*/



SELECT
    location,
    date,
    total_cases,
    total_deaths,
    (CAST(total_deaths AS FLOAT) / (total_cases)) * 100 AS case_fatality_rate
FROM PortfolioProject..CovidDeaths$
WHERE continent IS NOT NULL
  AND location like '%states%'
  AND total_cases IS NOT NULL
  AND total_deaths IS NOT NULL
ORDER BY 1,2;

-- Option 2: 

/*2. Replaced NULL with 0 using COALESCE

If you’d rather keep the rows that were removed by
 the WHERE clause in option 1 but treat NULL as 0: */


/* no NULLs appear in the output, 
but countries/dates with missing data will show 0 cases or 0 deaths.*/



SELECT
    location,
    date,
    COALESCE(total_cases, 0) AS total_cases,
    COALESCE(total_deaths, 0) AS total_deaths, /*so that the column should not be NULL column*/
    (CAST(COALESCE(total_deaths, 0) AS FLOAT) 
        / NULLIF(COALESCE(total_cases, 0), 0)) * 100 AS case_fatality_rate
FROM PortfolioProject..CovidDeaths$
WHERE continent IS NOT NULL
AND location LIKE '%states%'
ORDER BY location, date;


-- Total cases vs Population
-- Shows what percentage of population infected with Covid

SELECT 
	location,
	date,
	population,
	total_cases,
	(total_cases/population)*100 AS InfectedPercentage
FROM PortfolioProject..CovidDeaths$
--WHERE location like '%states%'
ORDER BY 1,2;





-- Countries with Highest Infection Rates compared to Population


SELECT 
	location,
	population,
	MAX(total_cases) AS HighestInfectionCount,
	MAX((total_cases/population))*100 AS InfectedPercentage
FROM PortfolioProject..CovidDeaths$
--WHERE location like '%states%'
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY InfectedPercentage DESC;


-- Showing Countries with the highest fatality count per population

SELECT 
	location,
	MAX(cast(total_deaths AS INT)) AS TotalFatalityCount
FROM PortfolioProject..CovidDeaths$
--WHERE location like '%states%'
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalFatalityCount DESC;

-- LET'S BREAK THINGS DOWN BY CONTINENT

-- Showing coninents with the highest fatality per population

SELECT 
	continent,
	MAX(cast(total_deaths AS INT)) AS TotalFatalityCount
FROM PortfolioProject..CovidDeaths$
--WHERE location like '%states%'
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalFatalityCount DESC;



-- GLOBAL NUMBERS

SELECT 
	
	SUM(new_cases) AS total_cases,
	SUM(CAST(new_deaths AS INT)) AS Total_Deaths, 
	SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS DeathPercentage
	--,total_deaths,(total_deaths/total_cases)*100 AS DeathPercentage
FROM PortfolioProject..CovidDeaths$
--WHERE location like '%states%'--
WHERE continent IS NOT NULL
--GROUP BY date
ORDER BY 1,2;



-- Using the Vaccination table:
-- Looking at Total Population vs. Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine


SELECT 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	COALESCE(vac.new_vaccinations, 0) AS newvacc,
	SUM(CONVERT(FLOAT,COALESCE(vac.new_vaccinations, 0)))
	   OVER (Partition BY dea.location ORDER BY dea.location, dea.date) AS RollingSUMVaccinations
	   --,(RollingSUMVaccinations/population)*100
FROM PortfolioProject..CovidDeaths$ AS dea
JOIN PortfolioProject..CovidVaccinations$ vac
 ON dea.location = vac.location
  AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
 ORDER BY 2,3;



/* This query joins CovidDeaths and CovidVaccinations tables,
then shows a cummulative total of vaccinations for each country,
ordered by location and date.

 We want to use the 'RollingSUMVaccinations' column results divided by the population to find
the percentage of people vaccinated out of the population.
How ? By using CTE
 */




-- Using CTE to perform Calculation on Partition By in previous query
-- Giving us percentage of vaccinated population

WITH PopvsVacc (Continent, Location, Date, Population, New_Vaccinations, RollingSUMVaccinations)
AS 
(SELECT 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	COALESCE(vac.new_vaccinations, 0) AS newvacc,
	SUM(CONVERT(FLOAT,COALESCE(vac.new_vaccinations, 0)))
	   OVER (Partition BY dea.location ORDER BY dea.location, dea.date) AS RollingSUMVaccinations
	   --,(RollingSUMVaccinations/population)*100
FROM PortfolioProject..CovidDeaths$ AS dea
JOIN PortfolioProject..CovidVaccinations$ vac
 ON dea.location = vac.location
  AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
 --ORDER BY 2,3;
 )
 SELECT *,
  (RollingSUMVaccinations/Population)*100 AS Vacc_Pop
 FROM PopvsVacc

 


-- Using Temp Table to perform Calculation on Partition By in previous query
DROP table if exists Percent_Population_Vaccinated
DROP table if exists PercentPopulationVaccinated 
DROP VIEW if exists  PercentPopulationVaccinated
CREATE TABLE Percent_Population_Vaccinated
(
Continent NVARCHAR(255),
location NVARCHAR(255),
Date DATETIME,
Population NUMERIC,
New_vaccinations NUMERIC,
Rolling_people_vaccinated NUMERIC
)

INSERT INTO Percent_Population_Vaccinated
SELECT 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	COALESCE(vac.new_vaccinations, 0) AS newvacc,
	SUM(CONVERT(FLOAT,COALESCE(vac.new_vaccinations, 0)))
	   OVER (Partition BY dea.location ORDER BY dea.location, dea.date) AS RollingSUMVaccinations
	   --,(RollingSUMVaccinations/population)*100
FROM PortfolioProject..CovidDeaths$ AS dea
JOIN PortfolioProject..CovidVaccinations$ vac
 ON dea.location = vac.location
  AND dea.date = vac.date
--WHERE dea.continent IS NOT NULL
 --ORDER BY 2,3;
  SELECT *,
  (Rolling_people_vaccinated/Population)*100 AS Vacc_Pop
 FROM Percent_Population_Vaccinated


 -- Creating View to store data for later visualisations

 CREATE VIEW PercentPopulationVaccinated AS
SELECT 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	COALESCE(vac.new_vaccinations, 0) AS newvacc,
	SUM(CONVERT(FLOAT,COALESCE(vac.new_vaccinations, 0)))
	   OVER (Partition BY dea.location ORDER BY dea.location, dea.date) AS RollingSUMVaccinations
	   --,(RollingSUMVaccinations/population)*100
FROM PortfolioProject..CovidDeaths$ AS dea
JOIN PortfolioProject..CovidVaccinations$ vac
 ON dea.location = vac.location
  AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
 --ORDER BY 2,3

 
 SELECT *
 FROM PercentPopulationVaccinated;





