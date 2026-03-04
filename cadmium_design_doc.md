# Cadmium

## AI-Assisted Portfolio Analytics and LTCG Harvesting Platform

### Overview

Cadmium is a locally hosted web application designed to aggregate stock
portfolio data from multiple Indian brokerage platforms and perform
automated tax analysis with a focus on Long-Term Capital Gains (LTCG)
harvesting.

The system collects holdings data through browser automation rather than
official APIs in order to keep the project zero-cost. Data is normalized
and analyzed by a tax engine, then optionally evaluated by a locally
running AI model to generate sell recommendations that optimize LTCG
harvesting under Indian tax law.

The application runs fully locally across machines in the same home
network.

------------------------------------------------------------------------

# Goals

Primary goals:

-   Aggregate holdings across Zerodha Kite, Zerodha Coin, Groww, and
    INDmoney
-   Determine holding periods and classify LTCG vs STCG
-   Compute realized and unrealized LTCG for the financial year
-   Identify gains that can be harvested under the ₹1.25L tax‑free LTCG
    allowance
-   Use a local AI model to suggest optimal stocks to sell and rebuy
-   Provide a clean web dashboard for portfolio insights

Secondary goals:

-   Resume-quality engineering architecture
-   Fully local deployment
-   Zero external paid services

------------------------------------------------------------------------

# High Level Architecture

System components:

Frontend (React + TypeScript)

Backend (FastAPI Python service)

Browser Automation Layer - Playwright or Puppeteer automation - Used to
authenticate brokers and scrape holdings

Portfolio Data Store - SQLite database initially - Stores normalized
holdings and transactions

Tax Engine - Computes capital gains classification and harvesting
capacity

AI Analysis Engine - Calls a local model running on another computer in
the LAN

------------------------------------------------------------------------

# Technology Stack

Backend - Python 3.11 - FastAPI - SQLAlchemy ORM - Pydantic - Async HTTP
clients

Browser Automation - Playwright (preferred) - Headless Chromium

Frontend - React - TypeScript - Vite - TailwindCSS

Visualization - Chart.js

Database - SQLite (initial) - Optional Postgres later

AI Integration - HTTP calls to local LLM service

------------------------------------------------------------------------

# Directory Structure

backend/ app/ main.py config.py database.py

        routers/
            portfolio_router.py
            broker_router.py
            tax_router.py
            ai_router.py

        services/
            portfolio_service.py
            broker_service.py
            tax_service.py
            ai_service.py

        brokers/
            zerodha_scraper.py
            groww_scraper.py
            indmoney_scraper.py
            coin_scraper.py

        models/
            holding.py
            transaction.py
            portfolio.py

        schemas/
            api_schemas.py

        utils/
            date_utils.py
            tax_utils.py

frontend/ src/ pages/ Dashboard.tsx Portfolio.tsx TaxAnalysis.tsx

        components/
            HoldingsTable.tsx
            TaxSummary.tsx
            HarvestRecommendations.tsx

        services/
            api.ts

------------------------------------------------------------------------

# Broker Data Collection

Since broker APIs are paid or restricted, Cadmium uses browser
automation.

Playwright will:

1.  Launch headless browser
2.  Navigate to login page
3.  Accept manual authentication (OTP / 2FA)
4.  Persist session cookies
5.  Scrape holdings tables

Each broker scraper implements a common interface:

fetch_holdings() fetch_transactions()

Data is converted to a unified holding schema.

------------------------------------------------------------------------

# Data Model

Holding

symbol isin broker quantity average_buy_price buy_date current_price
market_value unrealized_gain holding_period_days asset_type

Transaction

symbol date quantity price transaction_type

Portfolio

total_value total_unrealized_gain ltcg_eligible_value stcg_value

------------------------------------------------------------------------

# Tax Engine

Indian equity tax rules:

Holding period greater than 365 days = Long Term Capital Gain

Holding period less than 365 days = Short Term Capital Gain

Financial rule:

First ₹1.25L LTCG per financial year is tax exempt.

Tax Engine computes:

total_ltcg_realized total_ltcg_unrealized remaining_tax_free_ltcg
harvestable_gains

------------------------------------------------------------------------

# Harvesting Logic

The harvesting engine identifies:

Holdings eligible for LTCG Holdings with unrealized profit

Then calculates the quantity to sell so realized gains remain below the
tax‑free threshold.

Example:

Remaining exemption: ₹1.25L Stock unrealized gain: ₹40 per share

Suggested sale:

3125 shares → ₹1.25L realized gain

The recommendation is then refined by the AI model.

------------------------------------------------------------------------

# AI Model Integration

Cadmium sends structured portfolio data to a local AI model running on
another machine.

Example request:

POST /analyze

Payload:

holdings current gains remaining LTCG allowance

Expected response:

stocks_to_sell sell_quantities expected_gain reasoning

The AI system can factor in liquidity, volatility, and diversification
before suggesting trades.

------------------------------------------------------------------------

# Frontend Features

Dashboard

Total portfolio value Broker breakdown LTCG vs STCG visualization
Remaining tax free LTCG

Portfolio Page

Table of all holdings Gain / loss indicators Holding duration

Tax Analysis

Realized LTCG Unrealized LTCG Remaining exemption

Harvest Recommendations

AI suggested trades Estimated gains harvested Sell / rebuy plan

------------------------------------------------------------------------

# Security Considerations

Credentials stored in environment variables.

Session cookies encrypted locally.

No cloud storage of financial data.

Application runs only on localhost.

------------------------------------------------------------------------

# Deployment

Backend

localhost:8000

Frontend

localhost:5173

AI model

LAN machine accessible via HTTP endpoint

------------------------------------------------------------------------

# Future Improvements

Automated daily portfolio refresh

Trade execution automation

Multi-user support

Advanced tax simulation

Mobile interface

------------------------------------------------------------------------

# Project Value

Cadmium demonstrates:

Full-stack development

Financial systems engineering

Automation and web scraping

Local AI integration

Tax-aware investment analytics

The project is suitable as a portfolio piece for backend and systems
engineering roles.
