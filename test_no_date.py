#!/usr/bin/env python3
import requests
import os

def test_without_date(url, token=None):
    """Test API endpoint without any date parameters"""
    print(f"\n=== Testing without date: {url} ===")
    
    headers = {}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    
    # Test with no parameters at all
    print("Testing with NO parameters...")
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('content-type', 'unknown')}")
        
        if r.status_code == 200:
            print("✅ SUCCESS! API works without date parameter")
            try:
                data = r.json()
                print(f"Response type: {type(data)}")
                if isinstance(data, list):
                    print(f"Array length: {len(data)}")
                    if len(data) > 0:
                        print(f"First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'Not a dict'}")
                        print(f"Sample data: {data[0]}")
                elif isinstance(data, dict):
                    print(f"Response keys: {list(data.keys())}")
                    print(f"Sample data: {data}")
                print(f"Full response preview: {str(data)[:500]}...")
            except:
                print(f"Response preview: {r.text[:500]}...")
        elif r.status_code == 400:
            print("❌ Bad Request - API requires parameters")
            print(f"Error message: {r.text}")
        elif r.status_code == 401:
            print("🔐 Authentication required")
            print(f"Error message: {r.text}")
        elif r.status_code == 404:
            print("❌ Not Found")
        elif r.status_code == 500:
            print("⚠️ Server Error")
            print(f"Error message: {r.text}")
        else:
            print(f"❓ Unexpected status: {r.status_code}")
            print(f"Response: {r.text}")
            
    except requests.exceptions.Timeout:
        print("⏰ Timeout - endpoint might not be accessible")
    except requests.exceptions.ConnectionError:
        print("🔌 Connection Error")
    except Exception as e:
        print(f"💥 Error: {e}")

def main():
    # Get token from environment
    token = os.getenv('TULSA_ASSESSOR_TOKEN')
    if token:
        print(f"Found token: {token[:10]}...")
    else:
        print("No TULSA_ASSESSOR_TOKEN found in environment")
        print("You can set it with: export TULSA_ASSESSOR_TOKEN=your_token_here")
        token = None
    
    # Test both endpoints without date
    endpoints = [
        "https://api-assessor.tulsacounty.org/Modeling/GetAllValidSales",
        "https://api-assessor.tulsacounty.org/Modeling/GetAllLandLotParcelCharacteristics"
    ]
    
    for endpoint in endpoints:
        test_without_date(endpoint, token)

if __name__ == "__main__":
    main() 