import requests
import json
import os
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = "devcraft_admin_table3" 
table = dynamodb.Table(TABLE_NAME)

def send_email(email, group, Application_Link):
  url = "https://35ft2w96vg.execute-api.ap-south-1.amazonaws.com/prod/sendmail"

  # id 4 for the SES configuration
  ses_item_ses = get_ses_item("4")

  if ses_item_ses["Client_SES"] == "No":

    payload = json.dumps({
      "email": email,
      "group": group,
      "Application_Link": Application_Link
    })

    headers = {
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.text

  elif ses_item_ses["Client_SES"] == "Yes":

      res1 = send_application_access_email_from_payload(ses_item_ses.get("region", "ap-south-1"), email, group, Application_Link)

      return res1

  else:
      return {
        "statusCode": 400,
        "body": json.dumps({
          "message": "Invalid value for Client_SES"
        })

      }


def add_ses_details(domain, region, Client_SES):

    try:
        response = table.put_item(
            Item={
                'id': "4",
                'domain': domain,
                'region': region,
                'Client_SES': Client_SES,
                "sender_email": f"No_Reply@{domain}"
            }
        )
        return {
          "statusCode": 200,
          "body": json.dumps({
            "message": "Item stored successfully"
          })
        }
        

    except ClientError as e:
        print(f"Failed to store item: {e.response['Error']['Message']}")



def get_ses_item(id):
    """
    Retrieve an item from DynamoDB using the primary key 'id'.
    """
    try:
        response = table.get_item(
            Key={
                'id': id
            }
        )
        item = response.get('Item')
        if item:
            print(f"Item retrieved: {item}")
            return item
        else:
            print(f"No item found with id: {id}")
            return None
    except ClientError as e:
        print(f"Failed to retrieve item: {e.response['Error']['Message']}")
        return None



def send_application_access_email(region_name, email, group, application_link):
    """
    Send a concise application access email with improved email client compatibility
    """
    ses_client = boto3.client('ses', region_name= region_name)
    
    # Subject format: DevCraft [GroupName] Application Access Link
    subject = f"DevCraft {group} Application Access Link"

    body_html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* Reset styles for better email client compatibility */
            body, table, td, p, h1, h2, h3, h4, h5, h6 {{
                margin: 0;
                padding: 0;
            }}
            
            body {{
                font-family: Arial, Helvetica, sans-serif;
                color: #333333;
                background-color: #f9f9f9;
                line-height: 1.6;
                -webkit-text-size-adjust: 100%;
                -ms-text-size-adjust: 100%;
            }}
            
            /* Container table for email structure */
            .email-container {{
                width: 100%;
                max-width: 600px;
                margin: 0 auto;
                background-color: #ffffff;
                border-collapse: collapse;
            }}
            
            .outer-container {{
                width: 100%;
                background-color: #f9f9f9;
                padding: 20px 0;
            }}
            
            .header-cell {{
                padding: 30px 30px 20px 30px;
                text-align: center;
                background-color: #ffffff;
            }}
            
            .header-cell h2 {{
                color: #2c3e50;
                font-size: 24px;
                margin: 0;
                font-weight: bold;
            }}
            
            .content-cell {{
                padding: 10px 30px 20px 30px;
                text-align: center;
                background-color: #ffffff;
            }}
            
            .content-cell p {{
                font-size: 16px;
                margin: 15px 0;
                color: #555555;
            }}
            
            /* Button styling - using table for maximum compatibility */
            .button-table {{
                margin: 30px auto;
                border-collapse: collapse;
            }}
            
            .button-cell {{
                background-color: #667eea;
                border-radius: 8px;
                padding: 0;
                text-align: center;
            }}
            
            .button-link {{
                display: inline-block;
                padding: 16px 32px;
                background-color: #667eea;
                color: #ffffff !important;
                text-decoration: none;
                font-weight: bold;
                font-size: 16px;
                border-radius: 8px;
                text-transform: uppercase;
                letter-spacing: 1px;
                border: 2px solid #667eea;
                line-height: 1;
            }}
            
            /* Fallback button for clients that don't support the above */
            .button-fallback {{
                background-color: #667eea;
                color: #ffffff !important;
                padding: 16px 32px;
                text-decoration: none;
                font-weight: bold;
                font-size: 16px;
                border-radius: 8px;
                display: inline-block;
                text-transform: uppercase;
                letter-spacing: 1px;
                border: 2px solid #667eea;
                mso-padding-alt: 0;
                mso-text-raise: 4px;
            }}
            
            .footer-cell {{
                padding: 20px 30px;
                text-align: center;
                background-color: #ffffff;
                font-size: 14px;
                color: #6c757d;
            }}
            
            .disclaimer-cell {{
                padding: 0 30px 30px 30px;
                background-color: #ffffff;
            }}
            
            .disclaimer-box {{
                background-color: #f8f9fa;
                border-left: 4px solid #ffc107;
                padding: 15px;
                font-size: 14px;
                color: #6c757d;
                border-radius: 4px;
            }}
            
            /* Mobile responsive */
            @media only screen and (max-width: 600px) {{
                .email-container {{
                    width: 100% !important;
                    max-width: 100% !important;
                }}
                
                .header-cell,
                .content-cell,
                .footer-cell,
                .disclaimer-cell {{
                    padding-left: 20px !important;
                    padding-right: 20px !important;
                }}
                
                .button-link,
                .button-fallback {{
                    padding: 14px 28px !important;
                    font-size: 14px !important;
                }}
            }}
        </style>
        
        <!--[if mso]>
        <style type="text/css">
            .button-link {{
                padding: 16px 32px !important;
            }}
        </style>
        <![endif]-->
    </head>
    <body>
        <div class="outer-container">
            <table class="email-container" cellpadding="0" cellspacing="0" border="0">
                <!-- Header -->
                <tr>
                    <td class="header-cell">
                        <h2>🎉 Congratulations!</h2>
                    </td>
                </tr>
                
                <!-- Content -->
                <tr>
                    <td class="content-cell">
                        <p>We've got the access for the <strong>{group}</strong> group.</p>
                        <p>Click the button below to access your application:</p>
                        
                        <!-- Button using table structure for maximum compatibility -->
                        <table class="button-table" cellpadding="0" cellspacing="0" border="0">
                            <tr>
                                <td class="button-cell">
                                    <!--[if mso]>
                                    <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="{application_link}" style="height:54px;v-text-anchor:middle;width:200px;" arcsize="15%" stroke="f" fillcolor="#667eea">
                                    <w:anchorlock/>
                                    <center style="color:#ffffff;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;">🔗 Access Application</center>
                                    </v:roundrect>
                                    <![endif]-->
                                    
                                    <!--[if !mso]><!-->
                                    <a href="{application_link}" class="button-link" target="_blank" rel="noopener noreferrer">
                                        🔗 Access Application
                                    </a>
                                    <!--<![endif]-->
                                </td>
                            </tr>
                        </table>
                        
                        <!-- Additional fallback button -->
                        <div style="display: none;">
                            <a href="{application_link}" class="button-fallback" target="_blank" rel="noopener noreferrer">
                                🔗 Access Application
                            </a>
                        </div>
                    </td>
                </tr>
                
                <!-- Footer -->
                <tr>
                    <td class="footer-cell">
                        <p>Best regards,<br/>
                        <strong>DevCraft Team</strong></p>
                    </td>
                </tr>
                
                <!-- Disclaimer -->
                <tr>
                    <td class="disclaimer-cell">
                        <div class="disclaimer-box">
                            <strong>⚠️ Disclaimer:</strong> This access link is personalized for your application. Please do not share it with others. The link will expire after use or after the specified time period.
                        </div>
                    </td>
                </tr>
            </table>
        </div>
    </body>
    </html>
    """

    try:
        response = ses_client.send_email(
            Source='noreply@cloudthat.com',
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {
                    'Html': {'Data': body_html}
                }
            }
        )
        print(f"Access email sent to {email}. Message ID: {response['MessageId']}")
        return {
            'success': True,
            'message_id': response['MessageId'],
            'recipient': email
        }
    except Exception as e:
        print(f"Failed to send access email to {email}: {e}")
        return {
            'success': False,
            'error': str(e),
            'recipient': email
        }


def send_application_access_email_from_payload(region_name, email, group, Application_Link):
    """
    Main function to process payload and send access email
    """
    try:
        # Validate payload
        if not email:
            print("Error: No email provided in payload")
            return {'success': False, 'error': 'Email is required'}
        
        if '@' not in email or '.' not in email:
            print(f"Error: Invalid email format: {email}")
            return {'success': False, 'error': 'Invalid email format'}
        
        # Send email
        result = send_application_access_email(region_name, email, group, Application_Link)
        
        if result['success']:
            print(f"Access email sent successfully to {email}")
            print(f"Details: Group={{payload.get('group', 'Not specified')}}, Link={{payload.get('Application_Link', 'Default')}}")
            
        return result
        
    except Exception as e:
        print(f"Error in send_application_access_email_from_payload: {e}")
        return {'success': False, 'error': str(e)}
