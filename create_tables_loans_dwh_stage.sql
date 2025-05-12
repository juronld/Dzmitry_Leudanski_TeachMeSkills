/*
DROP TABLE IF EXISTS stage.Applications CASCADE;
DROP TABLE IF EXISTS stage.Loans CASCADE;
DROP TABLE IF EXISTS stage.Loan_Products CASCADE;
DROP TABLE IF EXISTS stage.Clients CASCADE;
DROP TABLE IF EXISTS stage.Job_Categories CASCADE;
DROP TABLE IF EXISTS stage.Credit_Scores CASCADE;
DROP TABLE IF EXISTS stage.Loan_Types CASCADE;
*/

-- Создание таблицы Loan_Types (Типы кредитов)
CREATE TABLE stage.Loan_Types (
    LoanTypeID SERIAL PRIMARY KEY,
    TypeName VARCHAR(100) NOT NULL,
	load_dttm TIMESTAMP DEFAULT now() NOT NULL  -- Время загрузки
);

-- Создание таблицы Credit_Scores (Кредитные рейтинги)
CREATE TABLE stage.Credit_Scores (
    CreditScoreID SERIAL PRIMARY KEY,
    Score INT NOT NULL CHECK (Score BETWEEN 300 AND 850),
	load_dttm TIMESTAMP DEFAULT now() NOT NULL  -- Время загрузки
);

-- Создание таблицы Job_Categories (Категории должности)
CREATE TABLE stage.Job_Categories (
    JobCategoryID SERIAL PRIMARY KEY,
    CategoryName VARCHAR(100) NOT NULL,
	load_dttm TIMESTAMP DEFAULT now() NOT NULL  -- Время загрузки
);

-- Создание таблицы Clients (Клиенты)
CREATE TABLE stage.Clients (
    ClientID SERIAL PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    DateOfBirth DATE NOT NULL,
    Email VARCHAR(100) NOT NULL UNIQUE,
    Phone VARCHAR(20),
    RegistrationDate TIMESTAMP NOT NULL,
    JobCategoryID INT NOT NULL,
    CreditScoreID INT NOT NULL,
	load_dttm TIMESTAMP DEFAULT now() NOT NULL,  -- Время загрузки
    FOREIGN KEY (JobCategoryID) REFERENCES stage.Job_Categories(JobCategoryID),
    FOREIGN KEY (CreditScoreID) REFERENCES stage.Credit_Scores(CreditScoreID)
);

-- Создание таблицы Loan_Products (Кредитные продукты)
CREATE TABLE stage.Loan_Products (
    LoanProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    LoanTypeID INT NOT NULL,
    InterestRate DECIMAL(5, 2) NOT NULL,
	load_dttm TIMESTAMP DEFAULT now() NOT NULL,  -- Время загрузки
    FOREIGN KEY (LoanTypeID) REFERENCES stage.Loan_Types(LoanTypeID)
);

-- Создание таблицы Applications (Заявки)
CREATE TABLE stage.Applications (
    ApplicationID SERIAL PRIMARY KEY,
    ClientID INT NOT NULL,
    ApplicationDate TIMESTAMP NOT NULL,
    RequestedAmount DECIMAL(15, 2) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    ProcessingTime INT NOT NULL,
    LoanProductID INT NOT NULL,
    PaymentRatio DECIMAL(5, 2) NOT NULL,
	load_dttm TIMESTAMP DEFAULT now() NOT NULL,  -- Время загрузки
    FOREIGN KEY (ClientID) REFERENCES stage.Clients(ClientID),
    FOREIGN KEY (LoanProductID) REFERENCES stage.Loan_Products(LoanProductID)
);

-- Создание таблицы Loans (Кредиты)
CREATE TABLE stage.Loans (
    LoanID SERIAL PRIMARY KEY,
    ApplicationID INT NOT NULL,
    Amount DECIMAL(15, 2) NOT NULL,
    StartDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP NOT NULL,
    Status VARCHAR(20) NOT NULL,
	load_dttm TIMESTAMP DEFAULT now() NOT NULL,  -- Время загрузки
    FOREIGN KEY (ApplicationID) REFERENCES stage.Applications(ApplicationID)
);

-- Создание индексов
CREATE INDEX idx_clients_fullname ON Clients (FullName);
CREATE INDEX idx_clients_email ON Clients (Email);
CREATE INDEX idx_applications_clientid ON Applications (ClientID);
CREATE INDEX idx_applications_status ON Applications (Status);
CREATE INDEX idx_loans_applicationid ON Loans (ApplicationID);
CREATE INDEX idx_loans_status ON Loans (Status);