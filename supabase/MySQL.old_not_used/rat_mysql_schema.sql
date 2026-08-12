CREATE DATABASE  IF NOT EXISTS `rat` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `rat`;
-- MySQL dump 10.13  Distrib 8.0.32, for Win64 (x86_64)
--
-- Host: localhost    Database: rat
-- ------------------------------------------------------
-- Server version	8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `images`
--

DROP TABLE IF EXISTS `images`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `images` (
  `image_no` text NOT NULL,
  `picture` blob,
  `entry_date` date DEFAULT NULL,
  `date_taken` date DEFAULT NULL,
  PRIMARY KEY (`image_no`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prompts`
--

DROP TABLE IF EXISTS `prompts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `prompts` (
  `prompt_field` text NOT NULL,
  `prompt_desc` text,
  PRIMARY KEY (`prompt_field`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ratbuilders`
--

DROP TABLE IF EXISTS `ratbuilders`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ratbuilders` (
  `Builder code` text NOT NULL,
  `Builder name` text,
  `Location` text,
  `Plant code` text,
  `Builder plant` text,
  `Remarks` text,
  PRIMARY KEY (`Builder code`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ratcatalogue`
--

DROP TABLE IF EXISTS `ratcatalogue`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ratcatalogue` (
  `image_no` text NOT NULL,
  `accession_no` double DEFAULT NULL,
  `category` text,
  `organisation_type` text,
  `country` text,
  `organisation` text,
  `route` text,
  `location` text,
  `circa` text,
  `imprecise_date` text,
  `date_taken` date DEFAULT NULL,
  `condition` text,
  `collection` text,
  `valuation` double DEFAULT NULL,
  `entry_date` date DEFAULT NULL,
  `owners_ref` text,
  `prints_allowed` text,
  `internet_use` text,
  `publications_use` text,
  `cd_no` text,
  `description` text,
  `picture` text,
  `gauge` text,
  `photographer` text,
  `cd_no_hr` text,
  `BW_image_no` text,
  `bw_cd_no` text,
  `active_area` text,
  `corporate_body` text,
  `facility` text,
  `Builder code` text,
  `Works number` text,
  `Year built` text,
  `Plant code` text,
  `Scountry` text,
  `layout` text,
  `sel_layout` text,
  `search` text,
  `Sorganisation` text,
  `Sroute` text,
  `Slocation` text,
  `Sactive_area` text,
  `Pcategory` text,
  `Builder` text,
  `Scorporate_body` text,
  `Sfacility` text,
  `SdescExtract` text,
  `Simage_no` text,
  `Sgauge` text,
  `Scollection` text,
  `Sbuilder code` text,
  `Sworks number` text,
  `CurrFldName` text,
  `HelpNotes` text,
  `Builder code2` text,
  `Builder name1` text,
  `Builder name2` text,
  `Plant code2` text,
  `Works number2` text,
  `Year built2` text,
  `Builder code3` text,
  `Plant code3` text,
  `Works number3` text,
  `Year built3` text,
  `Builder name3` text,
  `Password` text,
  `parent_folder` text,
  `imgref_stem` text,
  `website` text,
  PRIMARY KEY (`image_no`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ratcollections`
--

DROP TABLE IF EXISTS `ratcollections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ratcollections` (
  `collection` text NOT NULL,
  `owner` text,
  `donor` text,
  `storage_location` text,
  `photographer` text,
  `print_sales` text,
  `internet_use` text,
  `publications_use` text,
  `contact` text,
  `remarks` text,
  `accession_number` text,
  PRIMARY KEY (`collection`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ratcopyright`
--

DROP TABLE IF EXISTS `ratcopyright`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ratcopyright` (
  `name` text NOT NULL,
  `subject` text,
  `collection` text,
  `remarks` text,
  PRIMARY KEY (`name`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ratlabels`
--

DROP TABLE IF EXISTS `ratlabels`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ratlabels` (
  `image_no` text NOT NULL,
  `date` text,
  `description` text,
  `date1` text,
  PRIMARY KEY (`image_no`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ratroutes`
--

DROP TABLE IF EXISTS `ratroutes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ratroutes` (
  `start_location` text NOT NULL,
  `end_location` text NOT NULL,
  `organisation` text NOT NULL,
  `route` text NOT NULL,
  `country` text,
  `Remarks` text,
  PRIMARY KEY (`start_location`(100),`end_location`(100),`organisation`(100),`route`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed
