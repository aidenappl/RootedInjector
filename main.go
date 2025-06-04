package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
)

type FullFilingRecord struct {
	EIN        string      `json:"ein"`
	Name       string      `json:"name"`
	DLN        string      `json:"dln"`
	ObjectID   string      `json:"object_id"`
	XMLBatchID string      `json:"xml_batch_id"`
	Location   Address     `json:"location"`
	People     []People    `json:"people"`
	Form990    *IRSForm990 `json:"form_990"`
	Form990EZ  *IRS990EZ   `json:"form_990_ez"`
}

type People struct {
	PersonName   string   `json:"person_name"`
	PersonTitle  string   `json:"person_title"`
	PhoneNumber  *string  `json:"phone_number"`
	AverageHours *float64 `json:"average_hours"`
	Bookkeeper   bool     `json:"bookkeeper"`
	Compensation *int     `json:"compensation"`
	Address      *Address `json:"address"`
}

type IRS990EZ struct {
	GrossReceiptsAmt          int    `json:"gross_receipts_amt"`
	TotalRevenueAmt           int    `json:"total_revenue_amt"`
	TotalExpensesAmt          int    `json:"total_expenses_amt"`
	ExcessOrDeficitForYearAmt int    `json:"excess_or_deficit_for_year_amt"`
	PrimaryExemptPurpose      string `json:"primary_exempt_purpose_txt"`
	Website                   string `json:"website"`
}

type IRSForm990 struct {
	PrincipalOfficerName            string  `json:"principal_officer_name"`
	PrincipalOfficerAddress         Address `json:"principal_officer_address"`
	GrossReceiptsAmount             int     `json:"gross_receipts_amount"`
	WebsiteAddress                  string  `json:"website_address"`
	MissionDescription              string  `json:"mission_description"`
	FormationYear                   int     `json:"formation_year"`
	TotalAssetsEndOfYearAmount      int     `json:"total_assets_end_of_year_amount"`
	TotalLiabilitiesEndOfYearAmount int     `json:"total_liabilities_end_of_year_amount"`
}

type Address struct {
	AddressLine1 string `json:"address_line_1"`
	City         string `json:"city"`
	State        string `json:"state"`
	ZIPCode      string `json:"zip_code"`
}

func main() {
	var errorLog []string

	db, err := sql.Open("postgres", "***REMOVED***")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	file, err := os.Open("finished_records.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var records []FullFilingRecord
	err = json.NewDecoder(file).Decode(&records)
	if err != nil {
		log.Fatal(err)
	}

	bar := progressbar.Default(int64(len(records) - 1))

	for _, record := range records {
		tx, err := db.Begin()
		if err != nil {
			log.Fatal("begin tx:", err)
		}
		psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(tx)

		defer func() {
			if r := recover(); r != nil {
				_ = tx.Rollback()
				panic(r) // bubble up
			} else if err != nil {
				_ = tx.Rollback()
			}
			// if err is nil, Commit was successful and Rollback does nothing
		}()

		bar.Add(1)
		// Check if the record already exists
		var exists bool
		var dummy int
		err = psql.
			Select("1").
			From("website.organisations").
			Where(sq.Eq{"ein": record.EIN, "dln": record.DLN}).
			RunWith(db).
			QueryRow().
			Scan(&dummy)

		if err == sql.ErrNoRows {
			exists = false
		} else if err != nil {
			log.Fatal("check exists:", err)
			errorLog = append(errorLog, "duplicate found: "+err.Error())
		} else {
			exists = true
		}
		if exists {
			progressbar.Bprintf(bar, "[!!] Record with EIN %s and DLN %s already exists, overwriting\n", record.EIN, record.DLN)
			// delete the existing record
			_, err = psql.Delete("website.organisations").
				Where(sq.Eq{"ein": record.EIN, "dln": record.DLN}).
				RunWith(db).Exec()
			if err != nil {
				log.Fatal("delete existing record:", err)
				errorLog = append(errorLog, "delete failed: "+err.Error())
			}
			progressbar.Bprintf(bar, "[!!] Deleted existing record with EIN %s and DLN %s\n", record.EIN, record.DLN)
			// Reset exists to false to insert the new record
			exists = false
		}

		// Insert into organisations
		progressbar.Bprintf(bar, "[..] Inserting record with EIN %s and DLN %s\n", record.EIN, record.DLN)
		var orgID int
		if record.Form990EZ != nil {
			orgQuery := psql.Insert("website.organisations").
				Columns("name", "ein", "dln", "xml_batch_id", "website", "description").
				Values(record.Name, record.EIN, record.DLN, record.XMLBatchID,
					record.Form990EZ.Website, record.Form990EZ.PrimaryExemptPurpose).
				Suffix("RETURNING id")

			err = orgQuery.RunWith(db).QueryRow().Scan(&orgID)
			if err != nil {
				log.Fatal("org insert:", err)
				errorLog = append(errorLog, "org insert failed: "+err.Error())
			}
		} else {
			orgQuery := psql.Insert("website.organisations").
				Columns("name", "ein", "dln", "xml_batch_id").
				Values(record.Name, record.EIN, record.DLN, record.XMLBatchID).
				Suffix("RETURNING id")

			err = orgQuery.RunWith(db).QueryRow().Scan(&orgID)
			if err != nil {
				log.Fatal("org insert:", err)
				errorLog = append(errorLog, "org insert failed: "+err.Error())
			}
		}

		// Insert into organisation_locations
		_, err = psql.Insert("website.organisation_locations").
			Columns("organisation_id", "address_line_1", "city", "state", "zip_code").
			Values(orgID, record.Location.AddressLine1, record.Location.City,
				record.Location.State, record.Location.ZIPCode).
			RunWith(db).Exec()
		if err != nil {
			log.Fatal("location insert:", err)
			errorLog = append(errorLog, "location insert failed: "+err.Error())
		}

		// Insert into organisation_metadata
		if record.Form990EZ != nil {
			_, err = psql.Insert("website.organisation_metadata").
				Columns("organisation_id", "gross_reciepts_amt", "total_revenue_amt", "total_expenses_amt", "excess_or_deficit_for_year_amt").
				Values(orgID, record.Form990EZ.GrossReceiptsAmt, record.Form990EZ.TotalRevenueAmt,
					record.Form990EZ.TotalExpensesAmt, record.Form990EZ.ExcessOrDeficitForYearAmt).
				RunWith(db).Exec()
			if err != nil {
				log.Fatal("metadata insert:", err)
				errorLog = append(errorLog, "metadata insert failed: "+err.Error())
			}
		}

		// Insert people and optional addresses
		for _, person := range record.People {
			personQuery := psql.Insert("website.people").
				Columns("organisation_id", "bookkeeper", "name", "title", "phone_number", "average_hours", "compensation").
				Values(orgID, person.Bookkeeper, person.PersonName, person.PersonTitle,
					person.PhoneNumber, person.AverageHours, nullInt(person.Compensation)).
				Suffix("RETURNING id")

			var personID int
			err := personQuery.RunWith(db).QueryRow().Scan(&personID)
			if err != nil {
				log.Fatal("person insert:", err)
				errorLog = append(errorLog, "person insert failed: "+err.Error())
			}

			if person.Address != nil {
				_, err := psql.Insert("website.people_locations").
					Columns("person_id", "address_line_1", "city", "state", "zip_code").
					Values(personID, person.Address.AddressLine1, person.Address.City,
						person.Address.State, person.Address.ZIPCode).
					RunWith(db).Exec()
				if err != nil {
					log.Fatal("person location insert:", err)
					errorLog = append(errorLog, "person location insert failed: "+err.Error())
				}
			}
		}

		// If successful checkout
		err = tx.Commit()
		if err != nil {
			log.Fatal("commit tx:", err)
		}
		// Print success message
		progressbar.Bprintf(bar, "[OK] Successfully inserted record with EIN %s and DLN %s\n", record.EIN, record.DLN)
		progressbar.Bprintln(bar, "")
	}

	// Print any errors encountered
	if len(errorLog) > 0 {
		log.Println("⚠️ Errors encountered during processing:")
		errFile, err := os.Create("errors.txt")
		if err != nil {
			log.Printf("Failed to write error log: %v\n", err)
			return
		}
		defer errFile.Close()

		for _, line := range errorLog {
			_, _ = errFile.WriteString(line + "\n")
		}
		log.Printf("Wrote %d errors to errors.txt\n", len(errorLog))
	} else {
		log.Println("✅ All records inserted successfully — no errors.")
	}
}

// Helper to convert nullable int
func nullInt(i *int) interface{} {
	if i == nil {
		return nil
	}
	return *i
}

// Helper to convert nullable string
func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
