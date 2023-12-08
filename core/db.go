package db

import (
	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"kafka.exmaple/models"
)

var DB *gorm.DB

func InitDB() {
	var err error
	DB, err = gorm.Open("mysql", "root:root@tcp(localhost)/tutorial?parseTime=true")
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	// Auto Migrate
	DB.AutoMigrate(&models.Product{}, &models.Order{})
}
