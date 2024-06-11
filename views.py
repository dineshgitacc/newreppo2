""" Import Django libraries """
import datetime
from django.shortcuts import render
from django.conf import settings
from django.http import JsonResponse
from django.db.models import Count
""" Import rest framework libraries """
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet
from rest_framework import status
import logging

from log.serializers import JobMasterCreateSerializer, JobMasterViewSerializer, JobProcessDetailUpdateSerializer, JobProcessDetailsSerializer

from log.models import JobMaster, JobProcessDetails, StatusEnum

from log.controller.job_controller import JobController

# Get an instance of a logging
log = logging.getLogger(__name__)

class JobViewSet(ViewSet):

    def create(self, request):
        """Return a http response

        Optional plotz says to frobnicate the bizbaz first.
        """
        try:
            # Application sucess content
            log.info("Jobs create API")
            log.info(request.data)
            new_data = {}
            new_data['start_time']= request.data['start_time'] if request.data['start_time'] else None
            new_data['total_records']= request.data['total_records'] if request.data['total_records'] else 0
            new_data['processed_records']= request.data['processed_records'] if request.data['processed_records'] else 0
            new_data['failed_records']= request.data['failed_records'] if request.data['failed_records'] else 0
            new_data['pending_records']= request.data['pending_records'] if request.data['pending_records'] else 0
            new_data['reference_id']= request.data['reference_id']
            new_data['job_type']= request.data['job_type']
            new_data['request_from']= request.data['request_from']
            new_data['extra_data']= request.data['extra_data'] if request.data['extra_data'] else None
            new_data['status']= StatusEnum.New.value

            job_data = JobMasterCreateSerializer(data=new_data)

            if job_data.is_valid():
                obj = job_data.save()

                new_data['job_id']= obj.job_master_id
                response_content = {"error": False, "message": "Success", "status": 200, "data":new_data}
                log.info(response_content)
                return Response(response_content, status=status.HTTP_200_OK)
            else:
                response_content = {"error": True, "message": job_data.errors, "status": status.HTTP_400_BAD_REQUEST}
                log.error(response_content)
                return Response(response_content, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            # Application failure content
            log.error("Error in Jobs Create - {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request):
        """Return a http response

        Optional plotz says to frobnicate the bizbaz first.
        """
        try:
            # Application sucess content
            log.info("Jobs Update API")
            log.info(request.data)
            if "job_master_id" not in request.data or not request.data['job_master_id']: 
                return Response({"error": True, "message": "Input job master id missing ", "status": 400}, status=status.HTTP_400_BAD_REQUEST)
            
            jobmaster_set = JobMaster.objects.get(job_master_id = request.data['job_master_id'])
            print(jobmaster_set)
            if not jobmaster_set:
                return Response({"error": True, "message": "Record not found please enter valid ID", "status": 400}, status=status.HTTP_400_BAD_REQUEST)
    
            new_data = {}
            # new_data['start_time']= request.data['start_time'] if request.data['start_time'] else None
            if 'total_records' in request.data and request.data['total_records']:
                new_data['total_records']= request.data['total_records'] 
            if 'processed_records' in request.data and request.data['processed_records']:
                new_data['processed_records']= request.data['processed_records'] 
            if 'failed_records' in request.data and request.data['failed_records']:
                new_data['failed_records']= request.data['failed_records'] 
            if 'pending_records' in request.data and request.data['pending_records']:
                new_data['pending_records']= request.data['pending_records'] 
            if 'reference_id' in request.data and request.data['reference_id']:
                new_data['reference_id']= request.data['reference_id']
            if 'job_type' in request.data and request.data['job_type']:
                new_data['job_type']= request.data['job_type']
            if 'request_from' in request.data and request.data['request_from']:
                new_data['request_from']= request.data['request_from']
            if 'extra_data' in request.data and request.data['extra_data']:
                new_data['extra_data']= request.data['extra_data'] 
            if 'end_time' in request.data and request.data['end_time']:
                new_data['end_time']= request.data['end_time']
            if 'status' in request.data and request.data['status']:
                new_data['status']= request.data['status']

            job_data = JobMasterCreateSerializer(jobmaster_set, data=new_data)

            if job_data.is_valid():
                obj = job_data.save()

                new_data['job_id']= obj.job_master_id
                response_content = {"error": False, "message": "Success", "status": 200, "data":new_data}
                log.info(response_content)
                return Response(response_content, status=status.HTTP_200_OK)
            else:
                response_content = {"error": True, "message": job_data.errors, "status": status.HTTP_400_BAD_REQUEST}
                log.error(response_content)
                return Response(response_content, status=status.HTTP_400_BAD_REQUEST)
        except JobMaster.DoesNotExist:
            log.error("Error in Jobs Update - Requested record not found")
            return Response({"error": True, "message": "Requested record not found", "status": 200}, status=status.HTTP_200_OK)         
        except Exception as e:
            # Application failure content
            log.error("Error in Jobs Update - {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)

    def get_job_by_id(self, request, job_id):
        try:
            log.info("get_job_by_id API")
            log.info(job_id)
            job_obj = JobController()
            job_data= job_obj.get_log_data_by_id(job_id)
            response_content = {"error": False, "message": "Success", "status": 200, "data":job_data}
            log.info(response_content)
            return Response(response_content, status=status.HTTP_200_OK)
        except Exception as e:
            # Application failure content
            log.error("Error in Get jobs by ID- {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)

    def get_job_list(self, request):
        try:
            log.info("get_job_list API")
            log.info(request.data)
            job_data = []
            total_count = 0
            # limit = settings.LIMIT
            # offset = settings.OFFSET
            limit = 0
            offset = 0
            if 'offset' in request.data and request.data['offset']:
                offset = request.data['offset']
            if 'limit' in request.data and request.data['limit']:
                limit = request.data['limit']
            if 'job_id' in request.data and request.data['job_id']:
                total_count = JobMaster.objects.filter(job_master_id__in=request.data['job_id']).count()
                if limit !=0:
                    job = JobMaster.objects.filter(job_master_id__in=request.data['job_id']).order_by('-job_master_id')[int(offset):int(offset) + int(limit)]
                else:
                    job = JobMaster.objects.filter(job_master_id__in=request.data['job_id']).order_by('-job_master_id')
            else:
                total_count = JobMaster.objects.all().order_by('-job_master_id').count()      
                if limit !=0:
                    job = JobMaster.objects.all().order_by('-job_master_id')[int(offset):int(offset) + int(limit)]             
                else:
                    job = JobMaster.objects.all().order_by('-job_master_id')            
            
            job_serializer= JobMasterViewSerializer(job, many=True)
            if job_serializer.data:
                job_data= job_serializer.data
            response_content = {"error": False, "message": "Success", "status": 200, "count":total_count, "data":job_data}
            log.info(response_content)
            return Response(response_content, status=status.HTTP_200_OK)
        except JobMaster.DoesNotExist:
            log.error("Error in Get jobs List - Requested record not found")
            return Response({"error": True, "message": "Requested record not found", "status": 400}, status=status.HTTP_400_BAD_REQUEST)              
      
        except Exception as e:
            # Application failure content
            log.error("Error in Get jobs List- {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)


    def get_job_count(self, request):
        try:
            log.info("get_job_count API")
            log.info(request.data)
            count_data = []
            if 'job_id' in request.data and request.data['job_id']:
                count_data = JobMaster.objects.filter(job_master_id__in=request.data['job_id']).values('status').order_by('status').annotate(count=Count('status'))
            else:
                count_data = JobMaster.objects.all().values('status').order_by('status').annotate(count=Count('status'))
            
            response_content = {"error": False, "message": "Success", "status": 200, "data":count_data}
            log.info(response_content)
            return Response(response_content, status=status.HTTP_200_OK)
        except JobMaster.DoesNotExist:
            log.error("Error in Get job count - Requested record not found")
            return Response({"error": True, "message": "Requested record not found", "status": 400}, status=status.HTTP_400_BAD_REQUEST)              
      
        except Exception as e:
            # Application failure content
            log.error("Error in Get job count- {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)

class JobProcessViewSet(ViewSet):

    def create(self, request):
        """Return a http response

        Optional plotz says to frobnicate the bizbaz first.
        """
        try:
            # Application sucess content
            log.info("Job process create API")
            log.info(request.data)
            new_data = {}
            new_data['job_master_id']= request.data['job_id']
            new_data['start_time']= request.data['start_time'] if request.data['start_time'] else None
            new_data['total_records']= request.data['total_records'] if request.data['total_records'] else 0
            new_data['processed_records']= request.data['processed_records'] if request.data['processed_records'] else 0
            new_data['failed_records']= request.data['failed_records'] if request.data['failed_records'] else 0
            new_data['pending_records']= request.data['pending_records'] if request.data['pending_records'] else 0
            new_data['reference_id']= request.data['reference_id']
            new_data['job_type']= request.data['job_type']
            new_data['request_from']= request.data['request_from']
            new_data['extra_data']= request.data['extra_data'] if request.data['extra_data'] else None
            new_data['status']= StatusEnum.New.value

            job_process_data = JobProcessDetailsSerializer(data=new_data)

            if job_process_data.is_valid():
                obj = job_process_data.save()

                new_data['job_process_details_id']= obj.job_process_details_id
                response_content = {"error": False, "message": "Success", "status": 200, "data":new_data}
                log.info(response_content)
                return Response(response_content, status=status.HTTP_200_OK)
            else:
                response_content = {"error": True, "message": job_process_data.errors, "status": status.HTTP_400_BAD_REQUEST}
                log.error(response_content)
                return Response(response_content, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            # Application failure content
            log.error("Error in Job process create- {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request):
        """Return a http response

        Optional plotz says to frobnicate the bizbaz first.
        """
        try:
            # Application sucess content
            log.info("Job process update API")
            log.info(request.data)
            if "job_process_details_id" not in request.data or not request.data['job_process_details_id']: 
                return Response({"error": True, "message": "Input job process id missing ", "status": 400}, status=status.HTTP_400_BAD_REQUEST)
            
            jobprocess_set = JobProcessDetails.objects.get(job_process_details_id = request.data['job_process_details_id'])
            print(jobprocess_set)
            if not jobprocess_set:
                return Response({"error": True, "message": "Record not found please enter valid ID", "status": 400}, status=status.HTTP_400_BAD_REQUEST)

            new_data = {}
            if 'job_id' in request.data and request.data['job_id']:
                new_data['job_master_id']= request.data['job_id'] 
            if 'total_records' in request.data and request.data['total_records']:
                new_data['total_records']= request.data['total_records'] 
            if 'processed_records' in request.data and request.data['processed_records']:
                new_data['processed_records']= request.data['processed_records'] 
            if 'failed_records' in request.data and request.data['failed_records']:
                new_data['failed_records']= request.data['failed_records'] 
            if 'pending_records' in request.data and request.data['pending_records']:
                new_data['pending_records']= request.data['pending_records'] 
            if 'reference_id' in request.data and request.data['reference_id']:
                new_data['reference_id']= request.data['reference_id']
            if 'job_type' in request.data and request.data['job_type']:
                new_data['job_type']= request.data['job_type']
            if 'request_from' in request.data and request.data['request_from']:
                new_data['request_from']= request.data['request_from']
            if 'extra_data' in request.data and request.data['extra_data']:
                new_data['extra_data']= request.data['extra_data'] 
            if 'start_time' in request.data and request.data['start_time']:
                new_data['start_time']= request.data['start_time']
            if 'end_time' in request.data and request.data['end_time']:
                new_data['end_time']= request.data['end_time']
            if 'status' in request.data and request.data['status']:
                new_data['status']= request.data['status']

            new_data['updated_by'] = 1
            new_data['updated_date'] = datetime.datetime.now()
            job_process_data = JobProcessDetailUpdateSerializer(jobprocess_set, data=new_data)

            if job_process_data.is_valid():
                obj = job_process_data.save()
                new_data['job_process_details_id']= obj.job_process_details_id
                response_content = {"error": False, "message": "Success", "status": 200, "data":new_data}
                log.info(response_content)
                return Response(response_content, status=status.HTTP_200_OK)
            else:
                response_content = {"error": True, "message": job_process_data.errors, "status": status.HTTP_400_BAD_REQUEST}
                log.error(response_content)
                return Response(response_content, status=status.HTTP_400_BAD_REQUEST)
        except JobProcessDetails.DoesNotExist:
            log.error("Error in Job process update - Requested record not found")
            return Response({"error": True, "message": "Requested record not found", "status": 200}, status=status.HTTP_200_OK)
        except Exception as e:
            # Application failure content
            log.error("Error in Job process update- {}".format(str(e)))
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)
