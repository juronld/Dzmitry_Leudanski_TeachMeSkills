/*
DROP TABLE IF EXISTS Applications CASCADE;
DROP TABLE IF EXISTS Loans CASCADE;
DROP TABLE IF EXISTS Loan_Products CASCADE;
DROP TABLE IF EXISTS Clients CASCADE;
DROP TABLE IF EXISTS Job_Categories CASCADE;
DROP TABLE IF EXISTS Credit_Scores CASCADE;
DROP TABLE IF EXISTS Loan_Types CASCADE;
DROP TABLE IF EXISTS data_load_status CASCADE;
*/

-- Создание таблицы Loan_Types (Типы кредитов)
CREATE TABLE Loan_Types (
    LoanTypeID SERIAL PRIMARY KEY,
    TypeName VARCHAR(100) NOT NULL
);

-- Создание таблицы Credit_Scores (Кредитные рейтинги)
CREATE TABLE Credit_Scores (
    CreditScoreID SERIAL PRIMARY KEY,
    Score INT NOT NULL CHECK (Score BETWEEN 300 AND 850)
);

-- Создание таблицы Job_Categories (Категории должности)
CREATE TABLE Job_Categories (
    JobCategoryID SERIAL PRIMARY KEY,
    CategoryName VARCHAR(100) NOT NULL
);

-- Создание таблицы Clients (Клиенты)
CREATE TABLE Clients (
    ClientID SERIAL PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    DateOfBirth DATE NOT NULL,
    Email VARCHAR(100) NOT NULL UNIQUE,
    Phone VARCHAR(20),
    RegistrationDate TIMESTAMP NOT NULL,
    JobCategoryID INT NOT NULL,
    CreditScoreID INT NOT NULL,
    FOREIGN KEY (JobCategoryID) REFERENCES Job_Categories(JobCategoryID),
    FOREIGN KEY (CreditScoreID) REFERENCES Credit_Scores(CreditScoreID)
);

-- Создание таблицы Loan_Products (Кредитные продукты)
CREATE TABLE Loan_Products (
    LoanProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    LoanTypeID INT NOT NULL,
    InterestRate DECIMAL(5, 2) NOT NULL,
    FOREIGN KEY (LoanTypeID) REFERENCES Loan_Types(LoanTypeID)
);

-- Создание таблицы Applications (Заявки)
CREATE TABLE Applications (
    ApplicationID SERIAL PRIMARY KEY,
    ClientID INT NOT NULL,
    ApplicationDate TIMESTAMP NOT NULL,
    RequestedAmount DECIMAL(15, 2) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    ProcessingTime INT NOT NULL,
    LoanProductID INT NOT NULL,
    PaymentRatio DECIMAL(5, 2) NOT NULL,
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (LoanProductID) REFERENCES Loan_Products(LoanProductID)
);

-- Создание таблицы Loans (Кредиты)
CREATE TABLE Loans (
    LoanID SERIAL PRIMARY KEY,
    ApplicationID INT NOT NULL,
    Amount DECIMAL(15, 2) NOT NULL,
    StartDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP NOT NULL,
    Status VARCHAR(20) NOT NULL,
    FOREIGN KEY (ApplicationID) REFERENCES Applications(ApplicationID)
);



-- Создание таблицы статусов data_load_status
CREATE TABLE data_load_status (
    tablename varchar(100) NOT NULL,
    loaddate timestamp DEFAULT now() NOT NULL,
    status varchar(20) NOT NULL,
    recordcount INTEGER  
);

-- Создание индексов
CREATE INDEX idx_clients_fullname ON Clients (FullName);
CREATE INDEX idx_clients_email ON Clients (Email);
CREATE INDEX idx_applications_clientid ON Applications (ClientID);
CREATE INDEX idx_applications_status ON Applications (Status);
CREATE INDEX idx_loans_applicationid ON Loans (ApplicationID);
CREATE INDEX idx_loans_status ON Loans (Status);