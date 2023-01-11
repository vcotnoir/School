/*
 *		HEC Montréal
 *		TECH 60701 -- Technologies de l'intelligence d'affaires
 *		Session Automne 2022, Section J01
 *		TP_ETL
 *		Contact: Vincent Cotnoir (vincent.cotnoir@hec.ca)
 *		Matricule: 11323097
 *
 *	    Hypothèse: j'ai pris la décisision de me pas mettre de PK sur la table de fait meme si c'Est quelque chose qui pourrait etre Possible, Je ne vois pas necessairement le besoin
 *		car aucune référence à d'autres données et il est possible d'identifier les données facilement avec la date et le product number. Également ce n'était pas demandé.
 *
 *		Pour le flush, un simple PRINT a été utilisé puisque ce n'est pas pas une erreure et donc RAISERROR n'est pas utilisé. RAISERROR est utilisé pour le message ou les données sont déja intégrées.
 *		Commentaire: Difficile, mais bien interessant.
*/
USE master
GO
--Première Partie: Création de la base de données
DROP DATABASE IF exists tp_etl
CREATE DATABASE tp_etl
GO

USE tp_etl
GO
--Deuxième Partie: Création de la table de dimension et de fait dans la base de données.
DROP TABLE IF EXISTS Dim_date
CREATE TABLE Dim_date
(
	date_id INT IDENTITY(1,1) CONSTRAINT PK_dim_date PRIMARY KEY,
	[Date] DATETIME NOT NULL,
	[Month] TINYINT NOT NULL,
	[Weekday] TINYINT NOT NULL,
	[Year] SMALLINT NOT NULL,
	FiscalYear NUMERIC not null 
)
GO
DROP TABLE IF EXISTS Fact_sales_cube
CREATE TABLE Fact_sales_cube --est-ce qu'on crée une clée unique dans la table de fait.
(
	ProductNumber NVARCHAR(25),
	SubCategory NVARCHAR(50),
	Category NVARCHAR(50),
	Date_id INT not null CONSTRAINT FK_Dim_date_dat_id REFERENCES tp_etl.dbo.Dim_date(date_id) ,
	TotalQuantity FLOAT,
	TotalAmount MONEY,
	OrderCount FLOAT
)
GO

-- Troisième Partie: Procédure pour inserer les valeures dans la dimension de dates.
DECLARE  @beginning_date date = '2010-01-01',
         @ending_date date = '2015-01-01'
;WITH cte ([date]) AS (
	SELECT  @beginning_date as [date]
	UNION ALL
	SELECT CAST(DATEADD(day,1,[date]) as [date])
	FROM cte
	where date< @ending_date
)
insert into Dim_date (Date,Month,Weekday,Year,FiscalYear)
	SELECT *,
		DATEPART(MONTH,[date]) as [Month],
		DATEPART(WEEKDAY,[date]) as [Weekday],
		DATEPART(year,[date]) as [Year],
		case 
			when DATEPART(MONTH,[date])=4  then
				case
					when Date<(SELECT DATEADD(WEEK, DATEDIFF(WEEK, 0,DATEADD(DAY, 6 - DATEPART(DAY, [date]), [date])), 0)) then DATEPART(year,[date])-1
					else DATEPART(year,[date])
				end
			when DATEPART(MONTH,[date])<4 and DATEPART(year,[date])>DATEPART(year,[date])-1 then DATEPART(year,[date])-1
			When DATEPART(MONTH,[date])>4 then DATEPART(year,[date])
			else 0
		end as Fiscalyear
	FROM cte
	OPTION (MAXRECURSION 3000);
GO

--Quatrième Partie: Création de la Stored procedure qui permet d'appeler les différentes procédures voulues
-- première partie viens créer la fonction qui delete les données lorsque AFlusData=0
--le deuxième if fait deux choses: 1. regrde si les données existent, si oui, une erreure est levée. ensuite si non update de la table de fait.
CREATE or ALTER PROCEDURE sp_update_fact_table
--creation des variables qui seront utiles pour les different criteres
	@Month INT,
	@Year INT,
	@aFlushData BIT = 0
AS
BEGIN
--creation de l'etape qui nous permettera de retirer les donnes qui sont deja presentes
	IF(@aFlushData = 1)
		BEGIN
			BEGIN TRANSACTION
				BEGIN TRY
					-- retirer les lignes pour le mois et les années en variable
						DELETE
						FROM tp_etl.dbo.Fact_sales_cube 
						WHERE date_id between (SELECT MIN(date_id) FROM tp_etl.dbo.Dim_date WHERE [Month]=@Month and [Year]=@Year) and (SELECT MAX(date_id) FROM tp_etl.dbo.Dim_date WHERE [Month]=@Month and [Year]=@Year)
						PRINT(concat('Transaction réussi, les donnes du ',@year,'/',@month,' sont effaces dans la table de fait Fact_sales_cube.'))
				COMMIT TRANSACTION;
				END TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0--Renvoie le nombre d'événements de transaction survenus sur la connexion en cours.
					ROLLBACK TRANSACTION;
					RAISERROR ('les donnes du %i/%i ne sont pas retirées de la table de fait Fact_sales_cube.',17,-1,@year,@month);
				END CATCH
		END
--load des données si elles ne sont pas deja présentes
if(@aFlushData = 0)
	BEGIN--verification que la combinaison mois et année n'est pas deja présente
		IF EXISTS 
		(
			SELECT F.ProductNumber --seulement Productnumber est sélectionné puisque cela aide a diminuer la quantité de data a obtenir, SELECT * fonctionnes également.
			FROM tp_etl.dbo.Fact_sales_cube F 
			INNER JOIN tp_etl.dbo.Dim_date D ON D.date_id=F.Date_id
			WHERE D.Month=@Month and D.Year=@Year
			)
		BEGIN --si les données existent, le message d'erreur suivant apparait
			RAISERROR ('les donnes du %i/%i sont déjà chargés dans la table de fait Fact_sales_cube.',15,-1,@year,@month);
		END
	ELSE
	BEGIN --insertion des informations dans la table de fait
		BEGIN TRANSACTION
			BEGIN TRY
			/*
			Raisonnement: Deux CTE sont utilisés:
			la première est la table Product_CTE permet d'aller chercher toutes les valeures quantitatives demandées pour le devoir. elles sont calculées 
			par product number et par date. le select distinct est utilisé parce que la partition par date fesait en sorte que les valeures étaient répétées plusieures foir pour le meme
			product number qui était vendu à la meme date sur deux facture différentes.

			la deuxième est une table nommé Date_CTE qui ne fait que multiplier tous les produits a toutes les dates de la dimension date. les catégories et les sous-catégpries sont également ajoutés
			pour les produits a cette étape. cette table peut un petit peu etre vue comme une table de préparation.

			Finalement, pour l'insertion, la table Date_CTE est utilisé dans le FROM et est left joinned (pour garder toutes ces valeures même si aucun produit vendu pour une journée)
			avec Product_CTE sur la date et sur le produit. le join sur deux items permet d'insérer les bonnes données pour la bonne date et pour le bon produit spécifiquement.
			*/
				with Product_CTE (ProductNumber1,date_id1,total_ordered,Total_sold,Total_Per_Orders)--première CTE
					as
					(
					select distinct
						P.ProductNumber,
						D.date_id,
						sum(SOD.OrderQty) over(partition by P.ProductNumber,D.date_id) as total_ordered,
						sum(SOD.LineTotal) over(partition by P.ProductNumber, D.Date_id) as Total_sold,
						Count(SOD.SalesOrderID) over(partition by P.ProductNumber, D.Date_id) Total_Per_Orders 
					From adventureworks2019.sales.salesorderdetail SOD
						inner join adventureworks2019.sales.salesorderheader SOH on SOH.SalesOrderID=SOD.SalesOrderID
						inner join adventureworks2019.Production.Product P on P.ProductID=SOD.ProductID
						right join tp_etl.dbo.Dim_date D on D.date = SOH.OrderDate
					Where D.Month=@Month and D.Year=@Year
					GROUP BY D.date_id,P.ProductNumber, P.ProductID, SOD.SalesOrderID,SOD.LineTotal,SOD.OrderQty
					),
					Date_CTE(ProductNumber2,SubCat,Cat,date_id2)--deuxième CTE
					as
					(
					Select distinct
						P.ProductNumber,
						ISNULL(Sub.Name,'N/A') as SubCategory,
						ISNULL(cat.Name,'N/A') as Category,
						D.date_id
					from AdventureWorks2019.Production.Product P
						cross join tp_etl.dbo.Dim_date D
						left join AdventureWorks2019.Production.ProductSubcategory Sub on Sub.ProductSubcategoryID =P.ProductSubcategoryID
						left join AdventureWorks2019.Production.ProductCategory cat on cat.ProductCategoryID = sub.ProductCategoryID
					Where D.Month=@Month and D.Year=@Year 
					)
					INSERT INTO tp_etl.dbo.Fact_sales_cube--insert avec le left join.
					Select 
						Da.ProductNumber2,
						Da.Subcat,
						Da.cat,
						Da.Date_id2,
						ISNULL(PCTE.total_ordered,0),
						ISNULL(PCTE.Total_sold,0),
						ISNULL(PCTE.Total_Per_Orders,0)
					From Date_CTE Da
						Left join Product_CTE PCTE on PCTE.ProductNumber1 = Da.ProductNumber2 and PCTE.date_id1 = Da.Date_id2
					PRINT(concat('Table de fait Fact_sales_cube mise à jours avec les données du ',@year,'/',@month))
				COMMIT TRANSACTION;
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0--Renvoie le nombre d'événements de transaction survenus sur la connexion en cours.
				ROLLBACK TRANSACTION;
				RAISERROR ('les donnes du %i/%i ne sont pas chargées dans la table de fait Fact_sales_cube, erreure dinsertion.',15,-1,@year,@month);
			END CATCH
		END
	END
 END
 GO

--Appel pour ajouter les donnes
EXEC sp_update_fact_table @Month = 03, @Year = 2012
GO
--appel pour dire que les donnes sont deja dans la table
EXEC sp_update_fact_table @Month =03, @Year = 2012
GO
--appel pour retirer les donner qui dit qu'elles sont retires
EXEC sp_update_fact_table @Month = 03, @Year = 2012, @aFlushData=1
GO
-- Apel qui ajoute les donnes une autre fois
EXEC sp_update_fact_table @Month = 03, @Year = 2012
GO