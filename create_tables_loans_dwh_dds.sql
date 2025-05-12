-- DROP-запросы для всех таблиц (раскомментируйте при необходимости)
/*
DROP TABLE IF EXISTS dds.Loans CASCADE;
DROP TABLE IF EXISTS dds.Applications CASCADE;
DROP TABLE IF EXISTS dds.Loan_Products CASCADE;
DROP TABLE IF EXISTS dds.Clients CASCADE;
DROP TABLE IF EXISTS dds.Job_Categories CASCADE;
DROP TABLE IF EXISTS dds.Credit_Scores CASCADE;
DROP TABLE IF EXISTS dds.Loan_Types CASCADE;
*/

-- 1. Таблицы без SCD2 (простые справочники)
CREATE TABLE dds.Loan_Types (
    LoanTypeID SERIAL PRIMARY KEY,
    TypeName VARCHAR(100) NOT NULL
);

CREATE TABLE dds.Credit_Scores (
    CreditScoreID SERIAL PRIMARY KEY,
    Score INT NOT NULL CHECK (Score BETWEEN 300 AND 850)
);

-- 2. Таблицы с SCD2 (история изменений)
CREATE TABLE dds.Job_Categories (
    JobCategoryID SERIAL,
    CategoryName VARCHAR(100) NOT NULL,
    valid_from_dttm TIMESTAMP NOT NULL,
    valid_to_dttm TIMESTAMP NOT NULL,
    PRIMARY KEY (JobCategoryID, valid_from_dttm)
);

CREATE TABLE dds.Clients (
    ClientID SERIAL,
    FullName VARCHAR(100) NOT NULL,
    DateOfBirth DATE NOT NULL,
    Email VARCHAR(100) NOT NULL,
    Phone VARCHAR(20),
    RegistrationDate TIMESTAMP NOT NULL,
    JobCategoryID INT NOT NULL,
    CreditScoreID INT NOT NULL,
    valid_from_dttm TIMESTAMP NOT NULL,
    valid_to_dttm TIMESTAMP NOT NULL,
    PRIMARY KEY (ClientID, valid_from_dttm)
);

-- 3. Таблицы фактов (без SCD2)
CREATE TABLE dds.Loan_Products (
    LoanProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    LoanTypeID INT REFERENCES dds.Loan_Types(LoanTypeID),
    InterestRate DECIMAL(5, 2) NOT NULL
);

CREATE TABLE dds.Applications (
    ApplicationID SERIAL PRIMARY KEY,
    ClientID INT NOT NULL,
    ApplicationDate TIMESTAMP NOT NULL,
    RequestedAmount DECIMAL(15, 2) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    ProcessingTime INT NOT NULL,
    LoanProductID INT REFERENCES dds.Loan_Products(LoanProductID),
    PaymentRatio DECIMAL(5, 2) NOT NULL
);

CREATE TABLE dds.Loans (
    LoanID SERIAL PRIMARY KEY,
    ApplicationID INT REFERENCES dds.Applications(ApplicationID),
    Amount DECIMAL(15, 2) NOT NULL,
    StartDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP NOT NULL,
    Status VARCHAR(20) NOT NULL
);

-- Создание индексов
CREATE INDEX idx_clients_fullname ON dds.Clients (FullName);
CREATE INDEX idx_clients_email ON dds.Clients (Email);
CREATE INDEX idx_applications_clientid ON dds.Applications (ClientID);
CREATE INDEX idx_applications_status ON dds.Applications (Status);
CREATE INDEX idx_loans_applicationid ON dds.Loans (ApplicationID);
CREATE INDEX idx_loans_status ON dds.Loans (Status);