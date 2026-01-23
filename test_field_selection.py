#!/usr/bin/env python3
"""
Test script to verify field selection logic works correctly with real service outputs
"""

import json

# Real service outputs based on your examples
sample_record = {
    "job_id": "test-123",
    "row_id": 1,
    "input": {
        "name": "John Doe Jr.",
        "email": "LARSNE@MSN.COM",
        "phone": "555-1234"
    },
    "services": {
        "email_hygiene": {
            "status": "success",
            "input": "LARSNE@MSN.COM",
            "details": {
                "reference": "99",
                "indicator": "A",
                "email": "LARSNE@MSN.COM"
            }
        },
        "nameparse": {
            "name": "Doe Jr., John",
            "nameType": "M",
            "nameOrder": "last-name-first",
            "delimiter": ",",
            "output": {
                "family_score": 0,
                "family_name": "",
                "family_extra": "",
                "person1_score": 95,
                "person1_name": "",
                "person1_salutation": "",
                "person1_firstname": "John",
                "person1_middlename": "",
                "person1_nickname": "",
                "person1_lastname": "Doe",
                "person1_maturitytitle": "Jr.",
                "person1_title": "",
                "person1_gender": "M",
                "person1_extra": "",
                "person2_score": 0,
                "business_score": 0,
                "business_name": ""
            },
            "appendage": {
                "name_type1": "I",
                "surname1": "DOE",
                "given1": "JOHN",
                "middle1": "",
                "surname_suffix1": "JR",
                "title_code1": "1",
                "gender1": "M",
                "title1": "",
                "professional_title1": ""
            }
        },
        "us_address_lookup": {
            "output": {
                "firm": "DATA AXLE",
                "address1": "1290 CENTRAL PKWY W #500",
                "address2": "",
                "city": "MISSISSAUGA ON",
                "state": "",
                "zipcode": "",
                "zipfour": "",
                "carrierroute": "",
                "countyname": "",
                "errorstatus": "S00000",
                "statuscode": "S00000",
                "primaryaddress": "1290 CENTRAL PKWY W #500",
                "secondaryaddress": ""
            }
        },
        "ca_address_lookup": {
            "output": {
                "CACE_IFIRM": "Data Axle",
                "CACE_PRIM_RANGE": "1290",
                "CACE_PRIM_NAME": "CENTRAL",
                "CACE_SUFFIX": "PKY",
                "CACE_DIR": "W",
                "CACE_SEC_RANGE": "500",
                "CACE_CITY": "MISSISSAUGA",
                "CACE_PROVINCE": "ON",
                "CACE_POSTAL": "L5C4R3",
                "CACE_ERROR_CODE": "C4-4",
                "CACE_STAT_CODE": "S00000"
            }
        },
        "combined_suppression": {
            "output": {
                "_CS_AUDIT_recordid": "1",
                "_CS_AUDIT_Given_Initial": "K",
                "_CS_AUDIT_Given_Name": "Kristin",
                "_CS_AUDIT_Surname": "Cooper",
                "_CS_AUDIT_Gender": "",
                "_CS_AUDIT_Middle_Initial": "",
                "_CS_AUDIT_zip_code": "35004",
                "_CS_AUDIT_Street_Name": "WASHINGTON",
                "_CS_AUDIT_Primary_Number": "      1048",
                "_CS_AUDIT_Predirection": "",
                "_CS_AUDIT_Street_Designator": "DR",
                "_CS_AUDIT_Post_Direction": "",
                "_CS_AUDIT_Unit_Type": "",
                "_CS_AUDIT_Unit_Number": "",
                "_CS_AUDIT_State_Abbreviation": "AL",
                "_CS_AUDIT_Primary_Number_is_a_Box_Indicator": "N",
                "_CS_mps_pander": "N",
                "_CS_dts_pander": " ",
                "_CS_off_pander": " ",
                "_CS_bus_pander": "N",
                "_CS_dmi_pander": " ",
                "_CS_fds_pander": " ",
                "_CS_rel_pander": " ",
                "_CS_ext_pander": " ",
                "_CS_col_pander": "N",
                "_CS_mil_pander": "N",
                "_CS_trl_pander": "N",
                "_CS_ret_pander": "N",
                "_CS_nur_pander": "N",
                "_CS_cli_pander": " ",
                "_CS_dba_pander": " ",
                "_CS_aca_pander": "N",
                "_CS_email": "ANGELSORCE123@GMAIL.COM",
                "_CS_email_suppression": "N",
                "_CS_refresh_date": "         ",
                "_CS_supp_type": " ",
                "_CS_old_master_ind": " ",
                "_CS_ftc_match_indicator": "N",
                "_CS_ftc_run": "T",
                "_CS_ftc_date": "20251121",
                "_CS_tps_telephone": "N",
                "_CS_infousa_business_telephone": " ",
                "_CS_attorney_general_file": "N",
                "_CS_telephone": "2056406034"
            }
        }
    }
}

# Field selection matching the updated workflow payload
field_selection = {
    "email_hygiene": {
        "enabled": True,
        "fields": {
            "details.reference": True,
            "details.indicator": True,
            "details.email": False,
            "status": True,
            "input": False
        }
    },
    "nameparse": {
        "enabled": True,
        "fields": {
            "output.person1_firstname": True,
            "output.person1_lastname": True,
            "output.person1_gender": True,
            "output.person1_maturitytitle": True,
            "appendage.given1": True,
            "appendage.surname1": True,
            "appendage.gender1": True,
            "appendage.surname_suffix1": True,
            "name": True,
            "nameType": True
        }
    },
    "us_address_lookup": {
        "enabled": True,
        "fields": {
            "output.address1": True,
            "output.city": True,
            "output.state": True,
            "output.zipcode": True,
            "output.errorstatus": True,
            "output.primaryaddress": True
        }
    },
    "ca_address_lookup": {
        "enabled": True,
        "fields": {
            "output.CACE_IFIRM": True,
            "output.CACE_CITY": True,
            "output.CACE_PROVINCE": True,
            "output.CACE_POSTAL": True,
            "output.CACE_ERROR_CODE": True
        }
    },
    "combined_suppression": {
        "enabled": True,
        "fields": {
            "output._CS_AUDIT_Given_Name": True,
            "output._CS_AUDIT_Surname": True,
            "output._CS_AUDIT_zip_code": True,
            "output._CS_AUDIT_Street_Name": True,
            "output._CS_AUDIT_State_Abbreviation": True,
            "output._CS_mps_pander": True,
            "output._CS_bus_pander": True,
            "output._CS_email_suppression": True,
            "output._CS_ftc_match_indicator": True,
            "output._CS_tps_telephone": True,
            "output._CS_attorney_general_file": True,
            "output._CS_telephone": True
        }
    }
}

def get_nested_value(data: dict, field_path: str):
    """Get value from nested dictionary using dot notation"""
    keys = field_path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value

def filter_service_output(service_name: str, service_data: dict, field_selection: dict) -> dict:
    """Filter service output based on user-selected fields with support for nested paths"""
    if service_name not in field_selection or not field_selection[service_name].get("enabled", True):
        return {}
    
    selected_fields = field_selection[service_name].get("fields", {})
    filtered_output = {}
    
    # Process each selected field
    for field_path, include_field in selected_fields.items():
        if include_field:
            # Handle nested paths like 'details.reference' or 'output.person1_firstname'
            if '.' in field_path:
                value = get_nested_value(service_data, field_path)
                if value is not None:
                    # Create nested structure in output
                    keys = field_path.split('.')
                    current = filtered_output
                    for key in keys[:-1]:
                        if key not in current:
                            current[key] = {}
                        current = current[key]
                    current[keys[-1]] = value
            else:
                # Handle top-level fields
                if field_path in service_data:
                    filtered_output[field_path] = service_data[field_path]
    
    return filtered_output

def test_field_selection():
    print("🧪 Testing Field Selection Logic with Real Service Outputs")
    print("=" * 60)
    
    print("\n📋 Sample Record Structure:")
    print("Services:", list(sample_record["services"].keys()))
    
    print("\n🎯 Field Selection Results:")
    
    for service_name, service_data in sample_record["services"].items():
        print(f"\n🔧 {service_name.upper()}:")
        print("Original keys:", list(service_data.keys()))
        
        filtered = filter_service_output(service_name, service_data, field_selection)
        print("Filtered output:")
        print(json.dumps(filtered, indent=2))
        print("-" * 40)
    
    print("\n🎉 Test completed!")
    print("\n💡 This shows exactly what fields will appear in your final CSV!")

if __name__ == "__main__":
    test_field_selection()