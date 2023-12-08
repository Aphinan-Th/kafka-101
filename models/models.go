package models

import "github.com/jinzhu/gorm"

type Product struct {
	gorm.Model
	Name   string `gorm:"not null"`
	Amount int    `gorm:"not null"`
}

type Order struct {
	gorm.Model
	ProductID   uint
	UserLineUid string `gorm:"not null"`
	Status      string `gorm:"not null"`
}

type RequestData struct {
	ProductID int    `json:"productId"`
	UserID    string `json:"userId"`
	Topic     string `json:"topic"`
}
