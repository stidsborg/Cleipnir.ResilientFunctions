using System.Threading.Tasks;

namespace ConsoleApp.TravelAgency.ExternalServices;

public static class HotelBookingService
{
    public static void Start()
    {
        MessageBroker.Subscribe(msg =>
        {
            if (msg is BookHotel booking)
            {
                Task.Run(async () =>
                {
                    await Task.Delay(250);
                    await MessageBroker.Send(new HotelBooked(booking.BookingId));
                });
            }

            return Task.CompletedTask;
        }); 
    }
}