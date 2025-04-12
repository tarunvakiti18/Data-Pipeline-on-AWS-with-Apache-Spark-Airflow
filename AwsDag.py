from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'emr_spark_pipeline',
    default_args=default_args,
    description='Create EMR cluster, run Spark jobs, and keep cluster alive',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['emr', 'spark', 'airflow'],
)

JOB_FLOW_OVERRIDES = {
    "Name": "example-emr-cluster",
    "LogUri": "s3://<your-log-bucket>/logs",
    "ReleaseLabel": "emr-7.1.0",
    "ServiceRole": "<emr-service-role>",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        }
                    ]
                }
            }
        ],
        "Ec2KeyName": "<ec2-key-name>",
        "Ec2SubnetId": "<subnet-id>",
        "EmrManagedMasterSecurityGroup": "<master-security-group>",
        "EmrManagedSlaveSecurityGroup": "<slave-security-group>",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "AdditionalMasterSecurityGroups": [],
        "AdditionalSlaveSecurityGroups": [],
    },
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
        {"Name": "Spark"},
    ],
    "VisibleToAllUsers": True,
    "JobFlowRole": "<instance-profile-role>",
    "Tags": [
        {"Key": "project", "Value": "example-project"}
    ],
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "AutoTerminationPolicy": {
        "IdleTimeout": 60
    }
}

SPARK_STEPS = [
    {
        'Name': 'Run Spark Job 1',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'local[*]',
                '--class', 'com.example.MainClass1',
                's3://<your-code-bucket>/path/to/job1.jar'
            ]
        }
    },
    {
        'Name': 'Run Spark Job 2',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'local[*]',
                '--packages', 'org.example:your-dependency',
                '--class', 'com.example.MainClass2',
                's3://<your-code-bucket>/path/to/job2.jar'
            ]
        }
    },
    # Add more Spark jobs as needed
]

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    dag=dag,
)

add_spark_steps = EmrAddStepsOperator(
    task_id='add_spark_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag,
)

create_emr_cluster >> add_spark_steps
