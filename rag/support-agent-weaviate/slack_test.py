import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_slack_tokens():
    """Test Slack tokens using API endpoints"""
    
    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    app_token = os.environ.get("SLACK_APP_TOKEN")
    signing_secret = os.environ.get("SLACK_SIGNING_SECRET")
    
    print("🔍 Testing Slack API Tokens via Endpoints")
    print("=" * 50)
    
    # Test 1: Check if tokens exist
    if not bot_token:
        print("❌ SLACK_BOT_TOKEN not found")
        return
    
    if not app_token:
        print("❌ SLACK_APP_TOKEN not found")
        return
        
    if not signing_secret:
        print("❌ SLACK_SIGNING_SECRET not found")
        return
    
    print("✅ All environment variables found")
    
    # Test 2: Verify bot token format
    if not bot_token.startswith('xoxb-'):
        print("❌ Bot token doesn't start with 'xoxb-'")
        return
    
    if not app_token.startswith('xapp-'):
        print("❌ App token doesn't start with 'xapp-'")
        return
    
    print("✅ Token formats look correct")
    
    # Test 3: Test bot token with auth.test endpoint
    print("\n🧪 Testing bot token with auth.test...")
    
    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            "https://slack.com/api/auth.test",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('ok'):
                print("✅ Bot token is VALID!")
                print(f"   Workspace: {data.get('team')}")
                print(f"   User: {data.get('user')}")
                print(f"   User ID: {data.get('user_id')}")
                print(f"   Bot ID: {data.get('bot_id', 'N/A')}")
                print(f"   Team ID: {data.get('team_id')}")
                
                # Store bot_id for later use
                bot_id = data.get('bot_id')
                
                # Test 4: Get bot info if available
                if bot_id:
                    print(f"\n🤖 Getting bot details for {bot_id}...")
                    bot_response = requests.get(
                        f"https://slack.com/api/bots.info?bot={bot_id}",
                        headers=headers,
                        timeout=10
                    )
                    
                    if bot_response.status_code == 200:
                        bot_data = bot_response.json()
                        if bot_data.get('ok'):
                            bot_info = bot_data.get('bot', {})
                            print("✅ Bot info retrieved successfully!")
                            print(f"   Bot Name: {bot_info.get('name')}")
                            print(f"   App ID: {bot_info.get('app_id')}")
                            print(f"   Deleted: {bot_info.get('deleted', False)}")
                        else:
                            print(f"❌ Bot info failed: {bot_data.get('error')}")
                
            else:
                print(f"❌ Bot token is INVALID: {data.get('error')}")
                print("   Check your SLACK_BOT_TOKEN")
        else:
            print(f"❌ HTTP Error {response.status_code}")
            
    except requests.RequestException as e:
        print(f"❌ Network error: {e}")
    
    # Test 5: Basic API connectivity
    print(f"\n🌐 Testing basic API connectivity...")
    try:
        api_response = requests.post(
            "https://slack.com/api/api.test",
            timeout=10
        )
        
        if api_response.status_code == 200:
            api_data = api_response.json()
            if api_data.get('ok'):
                print("✅ Slack API is reachable!")
            else:
                print("❌ Slack API returned error")
        else:
            print(f"❌ API test failed with status {api_response.status_code}")
            
    except requests.RequestException as e:
        print(f"❌ API connectivity test failed: {e}")
    
    print("\n" + "=" * 50)
    print("Token testing complete!")

if __name__ == "__main__":
    test_slack_tokens()
